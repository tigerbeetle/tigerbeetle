#ifndef TB_CONTEXT_H_C
#define TB_CONTEXT_H_C

typedef struct TB_Context {
	uint8_t client [5120];
	uint8_t message_bus [144];
	uint8_t io [208];
	unsigned __int128 client_id;
} TB_Context;

int32_t init (struct TB_Context* const, const uint32_t cluster, char* const addresses, const uint32_t addresses_length);

typedef void (*results_callback)(void* const user_data, const uint8_t error, const uint8_t operation, uint8_t* const results, const uint32_t results_length);
int32_t request(struct TB_Context* const, void* const user_data, results_callback, const uint8_t operation, uint8_t* const batch_raw, const uint32_t batch_raw_length);
int32_t tick (struct TB_Context* const);
void deinit(struct TB_Context* const);
#endif