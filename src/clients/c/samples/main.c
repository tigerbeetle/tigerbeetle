#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include "../tb_client.h"

// config.message_size_max - @sizeOf(vsr.Header):
const int MAX_MESSAGE_SIZE = (1024 * 1024) - 128;

// Synchronization context between the callback and the main thread.
typedef struct completion_context {
    // Pointer to the reply's body.
    // In real usage, this memory should be copied to application's ownership before
    // the packet is returned to be reused in a next request.
    void* reply;
    int size;

    // In this example we synchronize using a condition variable:
    pthread_mutex_t lock;
    pthread_cond_t cv;

} completion_context_t;

void completion_context_init(completion_context_t *ctx);
void completion_context_destroy(completion_context_t *ctx);

// Sends and blocks the current thread until the reply arrives.
void send(
    tb_client_t client,
    tb_packet_list_t *packets,
    completion_context_t *ctx
);

// For benchmarking purposes.
long long get_time_ms(void);

// Completion function, called by tb_client no notify that a request as completed.
void on_completion(
    uintptr_t context, 
    tb_client_t client, 
    tb_packet_t *packet, 
    const uint8_t *data, 
    uint32_t size
) {    
    // The user_data gives context to a request:
    completion_context_t* ctx = (completion_context_t*)packet->user_data;

    // Signaling the main thread we received the reply:
    pthread_mutex_lock(&ctx->lock);
    ctx->size = size;
    ctx->reply = (void*)data;
    pthread_cond_signal(&ctx->cv);
    pthread_mutex_unlock(&ctx->lock);
}

int main(int argc, char **argv) {
    printf("TigerBeetle C Sample\n");
    printf("Connecting...\n");
    tb_client_t client;
    tb_packet_list_t packets;
    const char *address = "127.0.0.1:3000";

    // This sample is single-threaded,
    // In real use, &packets will return a linked list 
    // with multiple packets, for handling concurrent requests.   
    TB_STATUS status = tb_client_init(
        &client,          // Output client.
        &packets,         // Output packet list.
        0,                // Cluster ID.
        address,          // Cluster addresses.
        strlen(address),  //
        1,                // MaxConcurrency == 1, since this is a single-threaded example.
        NULL,             // No need for a global context.
        &on_completion    // Completion callback.
    );

    if (status != TB_STATUS_SUCCESS) {
        printf("Failed to initialize tb_client\n");
        exit(-1);
    }

    completion_context_t ctx;
    completion_context_init(&ctx);

    ////////////////////////////////////////////////////////////
    // Submitting a batch of accounts:                        //
    ////////////////////////////////////////////////////////////
    const int accounts_len = 2;
    const int accounts_size = sizeof(tb_account_t) * accounts_len;
    tb_account_t accounts[accounts_len];
    
    // Zeroing the memory, so we don't have to initialize every field.
    memset(&accounts, 0, accounts_size);
    
    accounts[0].id = 1;
    accounts[0].code = 2;
    accounts[0].ledger = 777;

    accounts[1].id = 2;
    accounts[1].code = 2;
    accounts[1].ledger = 777;
    
    // This sample is single-threaded,
    // In real use, this linked list will contain 
    // multiple packets, for handling multiple concurrent requests:
    printf("Creating accounts...\n");    
    packets.head->operation = TB_OPERATION_CREATE_ACCOUNTS;  // The operation to be performed.
    packets.head->data = &accounts;                          // The data to be sent.
    packets.head->data_size = accounts_size;                 //
    packets.head->user_data = &ctx;                          // User-defined context.
    packets.head->status = TB_PACKET_OK;                     // Will be set when the reply arrives.
    packets.tail = packets.head;

    send(client, &packets, &ctx);
    
    if (packets.head->status != TB_PACKET_OK) {
        // Checking if the request failed:
        printf("Error calling create_accounts (ret=%d)\n", packets.head->status);
        exit(-1);
    }

    if (ctx.size != 0) {
        // Checking for errors creating the accounts:
        tb_create_accounts_result_t *results = (tb_create_accounts_result_t*) ctx.reply;
        int results_len = ctx.size / sizeof(tb_create_accounts_result_t);
        printf("create_account results:\n");
        for(int i=0;i<results_len;i++) {
            printf("index=%d, ret=%d\n", results[i].index, results[i].result);
        }
        exit(-1);
    }
    printf("Accounts created successfully\n");
    
    ////////////////////////////////////////////////////////////
    // Submitting multiple batches of transfers:              //
    ////////////////////////////////////////////////////////////

    printf("Creating transfers...\n");
    const int max_batches = 100;
    const int transfers_per_batch = MAX_MESSAGE_SIZE / sizeof(tb_transfer_t);
    long max_latency_ms = 0;
    long total_time_ms = 0;
    for (int i=0; i< max_batches;i++) {
        tb_transfer_t transfers[transfers_per_batch];
        // Zeroing the memory, so we don't have to initialize every field.
        memset(&transfers, 0, MAX_MESSAGE_SIZE);
        
        for (int j=0; j< transfers_per_batch; j++) {
            transfers[j].id = j + 1 + (i * transfers_per_batch);
            transfers[j].debit_account_id = accounts[0].id;
            transfers[j].credit_account_id = accounts[1].id;
            transfers[j].code = 2;
            transfers[j].ledger = 777;
            transfers[j].amount = 1;
        }
        
        packets.head->operation = TB_OPERATION_CREATE_TRANSFERS;  // The operation to be performed.
        packets.head->data = &transfers;                          // The data to be sent.
        packets.head->data_size = MAX_MESSAGE_SIZE;               //
        packets.head->user_data = &ctx;                           // User-defined context.
        packets.head->status = TB_PACKET_OK;                      // Will be set when the reply arrives.
        packets.tail = packets.head;

        long long now = get_time_ms();
        send(client, &packets, &ctx);
  
        long elapsed_ms = get_time_ms() - now;
        if (elapsed_ms > max_latency_ms) max_latency_ms = elapsed_ms;
        total_time_ms += elapsed_ms;
        
        if (packets.head->status != TB_PACKET_OK) {
            // Checking if the request failed:
            printf("Error calling create_transfers (ret=%d)\n", packets.head->status);
            exit(-1);
        }

        if (ctx.size != 0) {
            // Checking for errors creating the accounts:
            tb_create_transfers_result_t *results = (tb_create_transfers_result_t*)ctx.reply;
            int results_len = ctx.size / sizeof(tb_create_transfers_result_t);
            printf("create_transfers results:\n");
            for(int i=0;i<results_len;i++) {
                printf("index=%d, ret=%d\n", results[i].index, results[i].result);
            }
            exit(-1);
        }
    }
    printf("Transfers created successfully\n");

	printf("============================================\n");

    printf("%d transfers per second\n", (max_batches * transfers_per_batch * 1000) / total_time_ms);
	printf("create_transfers max p100 latency per %d transfers = %dms\n", transfers_per_batch, max_latency_ms);
	printf("total %d transfers in %dms\n", max_batches * transfers_per_batch, total_time_ms);    
    printf("\n");

    ////////////////////////////////////////////////////////////
    // Looking up accounts:                                   //
    ////////////////////////////////////////////////////////////
    printf("Looking up accounts ...\n");
    tb_uint128_t ids[2];
    ids[0] = accounts[0].id;
    ids[1] = accounts[1].id;
    
    packets.head->operation = TB_OPERATION_LOOKUP_ACCOUNTS;
    packets.head->data = &ids;
    packets.head->data_size = sizeof(tb_uint128_t) * 2;
    packets.head->user_data = &ctx;
    packets.head->status = TB_PACKET_OK;
    packets.tail = packets.head;

    send(client, &packets, &ctx);
    
    if (packets.head->status != TB_PACKET_OK) {
        // Checking if the request failed:
        printf("Error calling lookup_accounts (ret=%d)", packets.head->status);
        exit(-1);
    }

    if (ctx.size == 0) {
        printf("No accounts found");
        exit(-1);
    } else {
        // Printing the account's balance:
        tb_account_t *results = (tb_account_t*)ctx.reply;
        int results_len = ctx.size / sizeof(tb_account_t);
        printf("Accounts:\n");
        printf("============================================\n");
        for(int i=0;i<results_len;i++) {
            printf("id=%d\n", results[i].id);
            printf("debits_posted=%d\n", results[i].debits_posted);
            printf("credits_posted=%d\n", results[i].credits_posted);
            printf("\n");
        }
    }

    // Cleanup
    completion_context_destroy(&ctx);
    tb_client_deinit(client);
}

// Sends and blocks the current thread until the reply arrives.
void send(
    tb_client_t client,
    tb_packet_list_t *packets,
    completion_context_t *ctx
) {
    // Locks the mutex:
    if (pthread_mutex_lock(&ctx->lock) != 0) {
        printf("Failed to lock mutex\n");
        exit(-1);
    }

    // Submits the request asynchronously:
    ctx->reply = NULL;
    ctx->size = 0;    
    tb_client_submit(client, packets);

    // Uses a condvar to sync this thread with the callback:
    while (ctx->reply == NULL) {
        if (pthread_cond_wait(&ctx->cv, &ctx->lock) != 0) {
            printf("Failed to wait condvar\n");
            exit(-1);
        }
    }
    
    if (pthread_mutex_unlock(&ctx->lock) != 0) {
        printf("Failed to unlock mutex\n");
        exit(-1);
    }
}

void completion_context_init(completion_context_t *ctx) {
    if (pthread_mutex_init(&ctx->lock, NULL) != 0) {
        printf("Failed to initialize mutex\n");
        exit(-1);
    }

    if (pthread_cond_init(&ctx->cv, NULL) != 0) {
        printf("Failed to initialize condition var\n");
        exit(-1);
    }
}

void completion_context_destroy(completion_context_t *ctx) {
    pthread_cond_destroy(&ctx->cv);
    pthread_mutex_destroy(&ctx->lock);
}

long long get_time_ms(void) {
    struct timeval tv;
    if (gettimeofday(&tv,NULL) != 0) {
        printf("Failed to get time of day\n");
        exit(-1);
    }
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}