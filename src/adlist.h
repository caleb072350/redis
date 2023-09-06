/* adlist.h redis 双端链表的实现 */

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

/* 双端链表节点
*/
typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;
} listNode;

/* 双端链表迭代器 */
typedef struct listIter {
    listNode *next;
    listNode *prev;
    int direction;
} listIter;

/* 双端链表结构 */
typedef struct list {
    listNode *head;
    listNode *tail;
    void *(*dup)(void *ptr);  // 节点值复制函数
    void (*free)(void *ptr);  // 节点值释放函数
    int (*match)(void *ptr, void *key);  // 节点值对比函数
    unsigned long len;        // 链表所包含的节点数量
} list;

/* 返回给定链表的节点数量 T = O(1) */
#define listLength(l) ((l)->len)

/* 返回给定链表的表头节点 T = O(1) */
#define listFirst(l) ((l)->head)

/* 返回给定链表的尾节点 T = O(1) */
#define listLast(l) ((l)->tail)

/* 返回给定节点的前置节点 */
#define listPrevNode(n) ((n)->prev)

/* 返回给定节点的后置节点 */
#define listNextNode(n) ((n)->next)

/* 返回给定节点的值 */
#define listNodeValue(n) ((n)->value)

/* 将链表 l 的值复制函数 设置为 m */
#define listSetDupMethod(l, m) ((l)->dup = (m))

/* 将链表 l 的值释放函数设置为 m */
#define listSetFreeMethod(l, m) ((l)->free = (m))

/* 将链表 l 的值对比函数设置为 m */
#define listSetMatchMethod(l, m) ((l)->match = (m))

/* 返回给定链表的值复制函数 */
#define listGetDupMethod(l) ((l)->dup)

/* 返回给定链表的值释放函数 */
#define listGetFree(l) ((l)->free)

/* 返回给定链表的值对比函数 */
#define listGetMatchMethod(l) ((l)->match)

/* Prototypes */
list *listCreate(void);
void listRelease(list *list);
list *listAddNodeHead(list *list, void *value);
list *listAddNodeTail(list *list, void *value);
// list *listInsertNode(list *list, listNode *old_node, void *value, int after);
void listDelNode(list *list, listNode *node);
listIter *listGetIterator(list *list, int direction);
listNode *listNextElement(listIter *iter);
void listReleaseIterator(listIter *iter);
list *listDup(list *orig);
listNode *listSearchKey(list *list, void *key);
listNode *listIndex(list *list, int index);
// void listRewind(list *list, listIter *iter);
// void listRewindTail(list *list, listIter *li);
// void listRotate(list *list);

#define AL_START_HEAD   0
#define AL_START_TAIL   1

#endif  