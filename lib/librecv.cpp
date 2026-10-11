#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "./include/lib.h"
#include "./include/utils.h"
#include "./include/protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>

#include <cstring>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int listen_sockfd = -1; //socket de listen pe portul 8032 unde sunt trimise SYN-urile
int recv_size = 0; //capacitatea bufferului de receptie
int recv_count = 0; //contor global pentru conn_id

//functie helper care trimite ACK catre client cu ack_num si recv_window
void send_ack_to_sender(struct connection *con) {
    struct poli_tcp_ctrl_hdr ctrl;
    memset(&ctrl, 0, sizeof(ctrl));
    ctrl.protocol_id = POLI_PROTOCOL_ID;
    ctrl.conn_id = con->conn_id;
    ctrl.type = CTRL;
    ctrl.ack_num = con->expected_seq;

    //calculam nr de segm libere
    int free_bytes = con->recv_buffer_max - (int)con->recv_buffer.size();
    int free_segments = free_bytes / MAX_DATA_SIZE;

    if (free_segments < 0) {
        free_segments = 0;
    }
    ctrl.recv_window = (uint16_t)free_segments;

    sendto(con->sockfd, &ctrl, sizeof(ctrl), 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */

    //asteapta pana cand avem date in buffer
    while (cons[conn_id]->recv_buffer.empty()) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(500);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    size = len;
    if (size > (int)cons[conn_id]->recv_buffer.size()) {
        size = (int)cons[conn_id]->recv_buffer.size();
    }

    memcpy(buffer, cons[conn_id]->recv_buffer.data(), size);

    //stergem datele consumate din buffer
    cons[conn_id]->recv_buffer.erase(
        cons[conn_id]->recv_buffer.begin(), cons[conn_id]->recv_buffer.begin() + size);

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        if (res > 0) {
            //am primit segm DATA de la client
            struct poli_tcp_data_hdr *data_hdr = (struct poli_tcp_data_hdr *)segment;

            if (data_hdr->protocol_id == POLI_PROTOCOL_ID && data_hdr->type == DATA) {
                uint16_t seq = data_hdr->seq_num;
                int payload_len = data_hdr->len;
                char *payload = segment + sizeof(struct poli_tcp_data_hdr);

                struct connection *con = cons[conn_id];

                if (seq == con->expected_seq) {
                    //punem segmanetul asteptat in buffer
                    con->recv_buffer.insert(con->recv_buffer.end(),
                                           payload, payload + payload_len);
                    con->expected_seq++;

                    //verificam daca avem segmente consecutive in out of order
                    while (con->out_of_order_buffer.count(con->expected_seq)) {
                        vector<char> &out_of_order = con->out_of_order_buffer[con->expected_seq];
                        con->recv_buffer.insert(con->recv_buffer.end(),
                            out_of_order.begin(), out_of_order.end());
                        con->out_of_order_buffer.erase(con->expected_seq);
                        con->expected_seq++;
                    }
                } else if (seq > con->expected_seq) {
                    int free_bytes = con->recv_buffer_max - (int)con->recv_buffer.size();
                    int window_size = free_bytes / MAX_DATA_SIZE;
                    if (window_size < 0) window_size = 0;

                    //acceptam doar daca seq se afla in interiorul ferestrei
                    if (seq < con->expected_seq + window_size) {
                        //salvam segmentul out of order
                        if (con->out_of_order_buffer.count(seq) == 0) {
                            con->out_of_order_buffer[seq] = vector<char>(payload, payload + payload_len);
                        }
                    }
                }
                //ignoram segmentul daca e duplicat sau in afara ferestrei, si trimitem ACK
                send_ack_to_sender(con);
            }
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) connection();

    int conn_id = recv_count++;
    con->conn_id = conn_id;

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */

    //asteptam SYN pe listen socket
    char syn_buf[MAX_SEGMENT_SIZE];
    struct sockaddr_in caddr;
    socklen_t clen = sizeof(caddr);

    int syn_received = 0;
    while (!syn_received) {
        int rc = recvfrom(listen_sockfd, syn_buf, sizeof(syn_buf), 0,
                               (struct sockaddr *)&caddr, &clen);
        if (rc > 0) {
            struct poli_tcp_ctrl_hdr *syn = (struct poli_tcp_ctrl_hdr *)syn_buf;
            if (syn->protocol_id == POLI_PROTOCOL_ID && syn->type == SYN) {
                syn_received = 1;
            }
        }
    }

    con->servaddr = caddr;

    //cream un socket nou pt aceasta conexiune si il legat la un port random
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    struct sockaddr_in data_addr;
    memset(&data_addr, 0, sizeof(data_addr));
    data_addr.sin_family = AF_INET;
    data_addr.sin_addr.s_addr = INADDR_ANY;
    data_addr.sin_port = 0;
    bind(con->sockfd, (struct sockaddr *)&data_addr, sizeof(data_addr));

    //aflam portul ales de kernel
    struct sockaddr_in bound_addr;
    socklen_t bound_len = sizeof(bound_addr);
    getsockname(con->sockfd, (struct sockaddr *)&bound_addr, &bound_len);
    uint16_t chosen_port = bound_addr.sin_port;

    //timeout pe socket pt handshake
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 300000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,
               &tv, sizeof(tv));

    //calculam recv_window initial
    con->recv_buffer_max = recv_size;
    int initial_recv_window = con->recv_buffer_max / MAX_DATA_SIZE;

    //trimitem SYN-ACK
    char synack_buf[sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    struct poli_tcp_ctrl_hdr *synack = (struct poli_tcp_ctrl_hdr *)synack_buf;
    memset(synack, 0, sizeof(*synack));
    synack->protocol_id = POLI_PROTOCOL_ID;
    synack->conn_id = conn_id;
    synack->type = SYN_ACK;
    synack->recv_window = (uint16_t)initial_recv_window;
    memcpy(synack_buf + sizeof(struct poli_tcp_ctrl_hdr),
           &chosen_port, sizeof(uint16_t));

    //retrimitem SYN-ACK pana primit ACK sau DAT
    int ack_received = 0;
    char ack_buf[MAX_SEGMENT_SIZE];

    while (!ack_received) {
        sendto(listen_sockfd, synack_buf, sizeof(synack_buf), 0,
               (struct sockaddr *)&caddr, clen);

        struct sockaddr_in from_addr;
        socklen_t from_len = sizeof(from_addr);
        int rc = recvfrom(con->sockfd, ack_buf, sizeof(ack_buf), 0,
                    (struct sockaddr *)&from_addr, &from_len);

        if (rc > 0) {
            struct poli_tcp_ctrl_hdr *ack = (struct poli_tcp_ctrl_hdr *)ack_buf;
            if (ack->protocol_id == POLI_PROTOCOL_ID && (ack->type == ACK || ack->type == DATA)) {
                //actualizam adresa clientului
                con->servaddr = from_addr;
                ack_received = 1;
            }
        }
    }

    //dezactivam timeout
    struct timeval no_timeout;
    no_timeout.tv_sec = 0;
    no_timeout.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,
               &no_timeout, sizeof(no_timeout));

    con->expected_seq = 0;
    con->send_base = 0;
    con->next_seq = 0;
    con->max_window_seq = 0;
    con->recv_window_from_server = 0;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;    

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    recv_size = recv_buffer_bytes;

    /* TODO: Create the connection socket and bind it to 8031 */

    //cream socket ul de listen si il legam la 8032 (scrie in cerinta)
    listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(listen_sockfd >= 0);

    int enable = 1;
    setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR,
        &enable, sizeof(int));

    struct sockaddr_in listen_addr;
    memset(&listen_addr, 0, sizeof(listen_addr));
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr.s_addr = INADDR_ANY;
    listen_addr.sin_port = htons(8032);

    int rc = bind(listen_sockfd, (struct sockaddr *)&listen_addr,
                  sizeof(listen_addr));
    assert(rc >= 0);

    pthread_t thread1;
    int ret;

    ret = pthread_create( &thread1, NULL,receiver_handler, NULL);
    assert(ret == 0);
}
