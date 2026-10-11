#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "./include/lib.h"
#include "./include/utils.h"
#include "./include/protocol.h"
#include <cassert>
#include <poll.h>
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

int lspeed = 0; //viteza
int ldelay = 0; //latenta
int send_count = 0; //contor global pt conn_id

//functie helper care trimite segm daca fereastra permite
void try_send_new_segments(struct connection *con)
{
    int inflight = (int)con->inflight_segments.size();

    //limita ferestrei = min(fereastra proprie, fereastra recv)
    int window_limit = con->max_window_seq;
    if (con->recv_window_from_server < window_limit) {
        window_limit = con->recv_window_from_server;
    }

    //permitem minim 1 segm
    if (window_limit < 1) {
        window_limit = 1;
    }

    //cat timp avem date si fereastra nu e plian
    while ((int)con->send_buffer.size() > 0 && inflight < window_limit) {
        //luam un chunk de max MAX_DATA_SIZE din bufferul de send
        int chunk_size = (int)con->send_buffer.size();
        if (chunk_size > MAX_DATA_SIZE) {
            chunk_size = MAX_DATA_SIZE;
        }

        //construim segm DATA
        char segment[MAX_SEGMENT_SIZE];
        struct poli_tcp_data_hdr *header = (struct poli_tcp_data_hdr *)segment;
        header->protocol_id = POLI_PROTOCOL_ID;
        header->conn_id = con->conn_id;
        header->type = DATA;
        header->seq_num = con->next_seq;
        header->len = chunk_size;

        memcpy(segment + sizeof(struct poli_tcp_data_hdr),
               con->send_buffer.data(), chunk_size);
        vector<char> payload(con->send_buffer.begin(),
                             con->send_buffer.begin() + chunk_size);
        con->inflight_segments[con->next_seq] = payload;
        con->send_buffer.erase(con->send_buffer.begin(),
                                con->send_buffer.begin() + chunk_size);

        sendto(con->sockfd, segment,
               sizeof(struct poli_tcp_data_hdr) + chunk_size, 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        con->next_seq++;
        inflight++;
    }
}

//functie helper care trimite un singur segm pt fast retrasmit
void retransmit_single_segment(struct connection *con)
{
    if (con->inflight_segments.empty()) {
        return;
    }

    auto iterator = con->inflight_segments.begin();
    uint16_t seq = iterator->first;
    vector<char> &payload = iterator->second;

    char segment[MAX_SEGMENT_SIZE];
    struct poli_tcp_data_hdr *header = (struct poli_tcp_data_hdr *)segment;
    header->protocol_id = POLI_PROTOCOL_ID;
    header->conn_id = con->conn_id;
    header->type = DATA;
    header->seq_num = seq;
    header->len = payload.size();

    memcpy(segment + sizeof(struct poli_tcp_data_hdr),
           payload.data(), payload.size());

    sendto(con->sockfd, segment,
           sizeof(struct poli_tcp_data_hdr) + payload.size(), 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

//functie helper care retrimite toate segm inflight
void retransmit_all_inflight(struct connection *con)
{
    for (auto &entry : con->inflight_segments) {
        uint16_t seq = entry.first;
        vector<char> &payload = entry.second;

        char segment[MAX_SEGMENT_SIZE];
        struct poli_tcp_data_hdr *header = (struct poli_tcp_data_hdr *)segment;
        header->protocol_id = POLI_PROTOCOL_ID;
        header->conn_id = con->conn_id;
        header->type = DATA;
        header->seq_num = seq;
        header->len = payload.size();

        memcpy(segment + sizeof(struct poli_tcp_data_hdr),
               payload.data(), payload.size());

        sendto(con->sockfd, segment,
               sizeof(struct poli_tcp_data_hdr) + payload.size(), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
    }
}

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */

    //verificam daca mai avem loc in buffer
    int space_left = SEND_BUFFER_CAPACITY - (int)cons[conn_id]->send_buffer.size();
    if (space_left <= 0) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }

    int to_copy = len;
    if (to_copy > space_left) {
        to_copy = space_left;
    }

    cons[conn_id]->send_buffer.insert(
        cons[conn_id]->send_buffer.end(), buffer, buffer + to_copy);
    size = to_copy;

    try_send_new_segments(cons[conn_id]);

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        if (res == -1) {
            //timeout
            retransmit_all_inflight(cons[conn_id]);
            try_send_new_segments(cons[conn_id]);
        } else if (res > 0) {
            //am ptimit un ACK de la server
            struct poli_tcp_ctrl_hdr *ctrl = (struct poli_tcp_ctrl_hdr *)buf;

            if (ctrl->protocol_id == POLI_PROTOCOL_ID && ctrl->type == CTRL) {
                uint16_t ack_num = ctrl->ack_num;
                cons[conn_id]->recv_window_from_server = ctrl->recv_window;

                if (ack_num > cons[conn_id]->send_base) {
                    //ACK nou
                    auto iterator = cons[conn_id]->inflight_segments.begin();
                    while (iterator != cons[conn_id]->inflight_segments.end()) {
                        if (iterator->first < ack_num) {
                            iterator = cons[conn_id]->inflight_segments.erase(iterator);
                        } else {
                            break;
                        }
                    }
                    cons[conn_id]->send_base = ack_num;
                    cons[conn_id]->duplicate_ack_count = 0;
                    cons[conn_id]->last_ack_received = ack_num;

                    //reetam timerul cand primim un ACK nou
                    for (int k = 0; k < fdmax; k++) {
                        if (data_fds[k].fd == cons[conn_id]->sockfd) {
                            struct itimerspec reset_spec;
                            reset_spec.it_value.tv_sec = 0;
                            reset_spec.it_value.tv_nsec = 50000000;
                            reset_spec.it_interval.tv_sec = 0;
                            reset_spec.it_interval.tv_nsec = 50000000;
                            timerfd_settime(timer_fds[k].fd, 0, &reset_spec, NULL);
                            break;
                        }
                    }
                } else if (ack_num == cons[conn_id]->last_ack_received) {
                    //ACK duplicat
                    cons[conn_id]->duplicate_ack_count++;
                    if (cons[conn_id]->duplicate_ack_count >= 3) {
                        retransmit_single_segment(cons[conn_id]);
                        cons[conn_id]->duplicate_ack_count = 0;
                    }
                }
            }
            try_send_new_segments(cons[conn_id]);
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) connection();

    int conn_id = send_count++;
    con->conn_id = conn_id;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */
    memset(&con->servaddr, 0, sizeof(con->servaddr));
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port = port;

    //setam timeout pe socket pt retrimiterea SYN
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 300000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,
               &tv, sizeof(tv));

    //trimitem SYN si asteptam SYN-ACK
    struct poli_tcp_ctrl_hdr syn_hdr;
    memset(&syn_hdr, 0, sizeof(syn_hdr));
    syn_hdr.protocol_id = POLI_PROTOCOL_ID;
    syn_hdr.conn_id = conn_id;
    syn_hdr.type = SYN;

    char synack_buf[sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    int synack_received = 0;

    while (!synack_received) {
        //trimitem SYN
        sendto(con->sockfd, &syn_hdr, sizeof(syn_hdr), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        //asteptam SYN-ACK
        struct sockaddr_in from_addr;
        socklen_t from_len = sizeof(from_addr);
        int rc = recvfrom(con->sockfd, synack_buf, sizeof(synack_buf),
                               0, (struct sockaddr *)&from_addr, &from_len);

        if (rc > 0) {
            struct poli_tcp_ctrl_hdr *synack =
                (struct poli_tcp_ctrl_hdr *)synack_buf;
            if (synack->protocol_id == POLI_PROTOCOL_ID &&
                synack->type == SYN_ACK) {
                //extragem portul si conn_id
                uint16_t data_port;
                memcpy(&data_port, synack_buf + sizeof(struct poli_tcp_ctrl_hdr),
                       sizeof(uint16_t));
                conn_id = synack->conn_id;
                con->conn_id = conn_id;

                //actualizam adresa serverului cu portul si salvam recv_window
                con->servaddr.sin_port = data_port;
                con->recv_window_from_server = synack->recv_window;
                synack_received = 1;
            }
        }
    }

    //trimitem ACK
    struct poli_tcp_ctrl_hdr ack_hdr;
    memset(&ack_hdr, 0, sizeof(ack_hdr));
    ack_hdr.protocol_id = POLI_PROTOCOL_ID;
    ack_hdr.conn_id = conn_id;
    ack_hdr.type = ACK;
    sendto(con->sockfd, &ack_hdr, sizeof(ack_hdr), 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    //dezactivam timeoutul pe socket
    struct timeval no_timeout;
    no_timeout.tv_sec = 0;
    no_timeout.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,
               &no_timeout, sizeof(no_timeout));

    con->send_base = 0;
    con->next_seq = 0;
    con->duplicate_ack_count = 0;
    con->last_ack_received = 0;

    //calculam fereastra optima: BDP / MAX_DATA_SIZE
    //BDP = speed(Mb/s) * RTT(ms) * 1000 / 8, RTT = 2 * delay
    long bdp = (long)lspeed * 1000000 / 8 * (2 * ldelay) / 1000;
    con->max_window_seq = bdp / MAX_DATA_SIZE;

    if (con->max_window_seq < 30) {
        con->max_window_seq = 30;
    }

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 50000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 50000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    lspeed = speed;
    ldelay = delay;

    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
