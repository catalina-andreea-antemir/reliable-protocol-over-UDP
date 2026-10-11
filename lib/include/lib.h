
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include <vector>
#include <map>

using namespace std;

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

/* Capacitate maxima pentru bufferul intern de trimitere (bytes) */
#define SEND_BUFFER_CAPACITY (128 * 1024)

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    /* TODO. Parameters used only by the sender */
    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */

    vector<char> send_buffer; //buffer de bytes netrimisi inca
    map<uint16_t, vector<char>> inflight_segments; //segm trimise dar neconfirmate
    uint16_t send_base; //primul seq neconfirmat
    uint16_t next_seq; //urmatorul seq nefolosit
    int recv_window_from_server; //numarul de segm
    int duplicate_ack_count; //contor de ACK uri duplicate consecutive
    uint16_t last_ack_received; //ultimul ACK primit

    /* TODO. Parameters used only by the client */
    uint16_t expected_seq; //seq urmator asteptat in ordine de la sender
    vector<char> recv_buffer; //buffer de bytes primiti in ordine
    int recv_buffer_max; //capacitatea maxima pntru bufferul de recv
    map<uint16_t, vector<char>> out_of_order_buffer; //segmente primite our of order
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
