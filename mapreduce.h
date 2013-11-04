/*Operating Systems
 *Map Reduce Using Thread and Processes
 *Zac Brown, Pintu Patel, Priya Mundra
 *10/6/2013
 */
#ifndef MAPREDUCE_H
#define MAPREDUCE_H

//////*DATA STRUCTURES*//////
typedef struct args_t{
    int num_maps;
    int num_reduces;
    char *input;
    char *output;
    int sort_type;
    int impl;
}ARGS;


/*KeyValue pair for our structure*/
typedef struct keyp_t{
    char *key;  //key is the word 
    int val; //val is the number of times word seen  
}KeyValue;

//GLOBAL
KeyValue **final;

/*Node of the linked list*/
typedef struct llnode_t{
    KeyValue *kv; //struct containing key and value
    struct llnode_t *next; //pointer to next node
}LLNode;

/*Linked List*/
typedef struct llist_t{
    LLNode *head; //pointer to just the head for now
    LLNode *tail;
}LinkedList;

typedef struct pdata_t{
    int start,end;
}ThreadData;


/* 	Functions 		*/

void checkArgs(int argc,char **argv,ARGS *args);		/* Check the command line args */
int (*COMPARE)(const void *,const void *);		/* This changes based on if wordcount/numsort*/
LinkedList *createLinkedList();				/* Create a Linked List */
KeyValue *createKeyValue(char *str, int v);		/* Create a Key Value struct */
KeyValue *createMappedKeyValue(char *str, int v);
LLNode *createNode(KeyValue *kval);  			/* Create a node */
char* concat(char *s1, char *s2);			/* concate a string 1  to string 2 */
void addToTail(LinkedList *list, LLNode *node);		/* add a node to a tail of a linked list */
int numcmp(char *num1,char *num2);			/* compare two numbers */
int strcmpi(char *a, char *b);				/* compare two strings */
void printLinkedList(LinkedList *List);			/* Prints Linked List */
void addInOrder(LinkedList *list, LLNode *node, int (*compare)(char *,char *)); 		/* Adds a LLnode in correct place in the linked list */
KeyValue *popFromHead(LinkedList *list);			/* Pops head from a list, and free the memory from the node */
int mergeLists(LinkedList *list1,LinkedList *list2,LinkedList *dest, int(*compare)(char *,char *)); 	/* Use Merge Sort merge two linked lists */
void writeLinkedList(LinkedList *List,char * output);
void recursiveDeleteNode(LLNode *cur);			/* Recursively Delete LLNode to free memory */
void deleteLinkedList(LinkedList *list); 		/* Deletes a Linked List */
int lineCount(const char *path);			/* Counts the number of lines in a file */
void reduceLinkedList(LinkedList *list); 			/* Reduce Linked List */
char *getChunkCommand(char *path, int line_count, int thread_count, int tid);
int sedWC(char *path,char type);
int getChunkBytes(char *path, int line_count, int thread_count, int tid);
LinkedList *createProcLL(char mastermem[8],char *file,int thread_count,int line_count);
FILE *getChunk(char *path, int line_count, int thread_count, int tid);  	/* Parse the file in to chunks to distribute to threads/processes */
int placeWordInBin(KeyValue *key);
int placeNumInBin(KeyValue *key);
void *mapKeyValue(void *args);				/* map function 	*/
void *reduce(void *args);				/* reduce function 	*/
void writeWords(LinkedList *List,char *output);			/* write to file for wordcount 	*/
void writeNums(KeyValue **keys,int size,char *output);			/* Write to file for intsort  	*/
LinkedList **splitList(LinkedList* original, int size, int num_reduce);	/* Split Linked List into smaller sizes for reduce to work on */
int compareWords(const void *p1, const void *p2);	/* Function to compare two strings/words */
int compareNums(const void *p1, const void *p2);		/* Function to comapre two numbers */
KeyValue **sortKeyValues(LinkedList *Global,int size,int (*compare)(const void *,const void *)); /* Sort the KeyValue in a Linked List */
void combineReducedLists(LinkedList *global, LinkedList *list);	/* combines a reduced linked list to a global linked list */
void combineLists(LinkedList* global, LinkedList *list);	/* combines two linked lists - add to the tail */
ThreadData *createThreadData(int i,int num_red, int size);	
/*For processes only*/
KeyValue *createKeyValue2(char *str, int v);
void *mapKeyValue2(void *args);

#endif
