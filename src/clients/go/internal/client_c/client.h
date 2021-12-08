#ifndef TB_CONTEXT_H_C
#define TB_CONTEXT_H_C

typedef struct TB_Context {} TB_Context;

int32_t init (const struct TB_Context*);

typedef void (*callback_fcn)(uint32_t);
int32_t request(const struct TB_Context*, callback_fcn);
#endif