/**
 * @author Stephan Hesse <stephan@emliri.com>
 * 
 * Plain C standard overflowable queue impl
 * Heavily inspired from https://codeforwin.org/2018/08/queue-implementation-using-array-in-c.html
 * 
 * */

#pragma once

typedef struct ExmgQueue {
    size_t head;
    size_t tail;
    size_t size;
    size_t len;
    void **q;
} ExmgQueue;

/*
    initialize queue pointers
*/
static void exmg_queue_init(ExmgQueue **queue_ptr, size_t size)
{
    ExmgQueue *queue = *queue_ptr = (ExmgQueue*) malloc(sizeof(ExmgQueue));
    queue->head = 0;
    queue->tail = 0;
    queue->len = 0;
    queue->size = size;

    size_t queue_bytes = size * sizeof(void*);
    queue->q = malloc(queue_bytes);
    memset(queue->q, 0, queue_bytes);
}

static void exmg_queue_deinit(ExmgQueue **queue)
{
    if (*queue == NULL) return;
    free((*queue)->q);
    free(*queue);
    *queue = NULL;
}

/*
    return 1 if queue is full, otherwise return 0
*/
static int exmg_queue_is_full(ExmgQueue *queue)
{
    return queue->len == queue->size;
}

/*
  return 1 if the queue is empty, otherwise return 0
*/
static int exmg_queue_is_empty(ExmgQueue *queue)
{
    return queue->len == 0;
}

static size_t exmg_queue_length(ExmgQueue *queue)
{
    return queue->len;
}

static void* exmg_queue_peek(ExmgQueue *queue)
{
    if (exmg_queue_is_empty(queue)) return NULL;
    return queue->q[queue->head];
}

/*
   enqueue an element
   precondition: the queue is not full
*/
static int exmg_queue_push(ExmgQueue *queue, void* element)
{
    if (exmg_queue_is_full(queue)) return 0;

    queue->q[queue->tail] = element;
    queue->tail = (queue->tail + 1) % queue->size;
    queue->len++;
    return 1;
}

/*
    dequeue an element
    precondition: queue is not empty
*/
static void* exmg_queue_pop(ExmgQueue *queue)
{
    if (exmg_queue_is_empty(queue)) return NULL;

    void* data = queue->q[queue->head];
    queue->head = (queue->head + 1) % queue->size;
    queue->len--;
    return data;
}