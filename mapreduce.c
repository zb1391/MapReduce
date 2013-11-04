/*Operating Systems
 *Map Reduce Using Thread and Processes
 *Zac Brown, Pintu Patel, Priya Mundra
 *10/6/2013
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <err.h>
#include <pthread.h>
#include <sys/shm.h>
#include <semaphore.h>
#include <ctype.h>
#include "mapreduce.h"

void checkArgs(int argc,char **argv,ARGS *args){

    if(argc!=13){
	printf("Error, incorrect number of arguments\n");
	exit(-1);
    }
    if(strcmp("--app",argv[1])!=0){
	printf("first arg must be --app\n");
	exit(-1);
    }
   
   if(strcmpi("wordcount",argv[2])!=0 && strcmpi("sort",argv[2])!=0){
        printf("second arg is %s\n",argv[2]);
	printf("second arg must be wordcount or sort\n");
	exit(-1);
    }
    else{
	printf("argv[2] was wordcount or sort\n");
	if(strcmpi("wordcount",argv[2])==0)
	    args->sort_type=0;
	else
	    args->sort_type=1;
    }
    if(strcmp("--impl",argv[3])!=0){
	printf("3rd arg must be --impl\n");
	exit(-1);
    }

   if(strcmp("procs",argv[4])!=0 && strcmp("threads",argv[4])!=0){
	printf("4th arg must be procs or threads\n");
	exit(-1);
   }
   else{
	printf("argv[4] was threads or processes\n");
	if(strcmpi("threads",argv[4])==0)
	    args->impl=0;
	else
	    args->impl=1;
    }
    if(strcmp("--maps",argv[5])!=0){
	printf("5th arg must be --maps\n");
	exit(-1);
    }
    sscanf(argv[6],"%d",&(args->num_maps));
    if(strcmp("--reduces",argv[7])!=0){
	printf("7th arg must be --reduces\n");
	exit(-1);
    }
    sscanf(argv[8],"%d",&(args->num_reduces));
    if(strcmp("--input",argv[9])!=0){
	printf("9th arg must be --input\n");
	exit(-1);
    }
    args->input=argv[10];
    if(strcmp("--output",argv[11])!=0){
	printf("11th arg must be --output\n");
	exit(-1);
    }
    args->output=argv[12];
    printf("args are acceptable\n");

}

/*DATA STRUCTURE METHODS*/
LinkedList *createLinkedList(){
    LinkedList *list = (LinkedList*)malloc(sizeof(LinkedList));
    list->head=NULL;
    list->tail=NULL;
    return list;
}

KeyValue *createKeyValue(char *str, int v){
    KeyValue *kv = (KeyValue*)malloc(sizeof(KeyValue));
    kv->key=(char*)malloc(sizeof(char)*strlen(str)+1);
    kv->val=v;
    strcpy(kv->key,str);
    return kv;
}

KeyValue *createKeyValue2(char *str, int v){
    KeyValue *kv = (KeyValue*)malloc(sizeof(KeyValue));
    kv->key=(char*)malloc(sizeof(char)*strlen(str)+2);
    kv->val=v;
    strcpy(kv->key,str);
    //char *c;
    //int i;
    kv->key[strlen(str)]=' ';
    return kv;
}

KeyValue *createMappedKeyValue(char *str, int v){
    KeyValue *kv = (KeyValue*)malloc(sizeof(KeyValue));
    kv->key=(char*)malloc(sizeof(char)*strlen(str)+1);
    kv->val=v;
    strcpy(kv->key,str);
    return kv;
}

LLNode *createNode(KeyValue *kval){
    LLNode *node = (LLNode*)malloc(sizeof(LLNode));
    node->kv = kval;
    node->next=NULL;
    return node;
}

void addToHead(LinkedList *list,LLNode *node){
    node->next = list->head;
    list->head=node;

    if(list->tail==NULL){
	list->tail=list->head;
    }
}

char* concat(char *s1, char *s2)
{
    char *result = malloc(strlen(s1)+strlen(s2)+1);//+1 for the zero-terminator
    //in real code you would check for errors in malloc here
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}

void addToTail(LinkedList *list, LLNode *node){
    /*if there is a tail that isnt NULL, add the new node to the tail*/
    if(list->tail!=NULL){
	list->tail->next = node;
	list->tail = list->tail->next;
    }
    /*this is only when the list is empty*/
    else{
	list->head = node;
        list->tail=node;
    }

}
int numcmp(char *num1,char *num2){
    int a,b;
    a = atoi(num1);
    b = atoi(num2);
    //printf("in NUMCMP, comparing %s and %s\n",num1,num2);
    if(a>b){
        //printf("%d greater than %d\n",a,b);
	return 1;
    }
    else if(a<b){
	//printf("%d is less than %d\n",a,b);
	return -1;
    }
    else
	return 0;
}
int strcmpi(char *a, char *b){
   for (;; a++, b++) {
	int d = tolower(*a) - tolower(*b);
        if (d != 0 || !*a)
            return d;
    }
}

void printLinkedList(LinkedList *List){
    LLNode *cur = List->head;
    while(cur!=NULL){
	printf("<%s,%d>\n",cur->kv->key,cur->kv->val);
	cur = cur->next;
    }
}


/*adds LLNode node in proper order to list. This function has a fourth parameter
 *called comare. this is a pointer to the comparison method we want to use.
 *if we want to do word_count we pass strcmpi()
 *if we want to do int_sort we pass numcmp()
 */
void addInOrder(LinkedList *list, LLNode *node, int (*compare)(char *,char *)){
    LLNode *last = NULL;
    LLNode *where = list->head;
    if(list->head){
	while(where && compare(node->kv->key,where->kv->key)>0){
	    last = where;
	    where = where->next;
	}
	if(!last){
	    /*compare using the proper comparison function*/
	    if(compare(node->kv->key,list->head->kv->key)>0){
		list->head->next=node;
	    }
	    else{
	        node->next=list->head;
		list->head=node;
            }
	}
	else{
		node->next=last->next;
		last->next=node;
	}
    }
    else{
	addToHead(list,node);
    }
    //printf("ADDINORDER list is:\n");
    //printLinkedList(list);
}


/*
 *Pop the KeyValue off the LinkedList's head and free's the memory from the node
 *DONT FORGET TO FREE THE KEYVALUE ONCE YOURE DONE WITH IT!
 */
KeyValue *popFromHead(LinkedList *list){
    KeyValue *keyval = NULL;
    LLNode *cur = list->head;
    if(list->head){
	/*if head has a next node*/
	if(cur->next){
	    /*get kv, set head to next and free the old head*/
	    keyval=cur->kv;
	    list->head=cur->next;
	    free(cur);
        }
        else{
	    /*since list's head doesnt have next, we want to set the List's head to point to NULL again*/
	    list->head=NULL;
	    /*This just checks to make sure we didnt try to pop from an empty List*/
	    keyval=cur->kv;
	    free(cur);
        }
    }
    else{
	//printf("TRYING TO POP FROM EMPTY LIST!\n");
    }
    return keyval;
}

/*given two linked lists, it performs a merge sort, placing the final result in dest.
 *the last parameter is a pointer to the proper compare function
 *if we want word count we pass strcmpi()
 *if we want int sort we pass numcmp()
 */
int mergeLists(LinkedList *list1,LinkedList *list2,LinkedList *dest, int(*compare)(char *,char *)){
    KeyValue *fromList1,*fromList2;
    int cmp;
    int listSize = 0;

    fromList1 = popFromHead(list1);
    fromList2 = popFromHead(list2);
    //printf("Inside while loop:\n");
    while(fromList1 != NULL && fromList2 != NULL){
	//printf("comparing %s with %s\n",fromList1->key, fromList2->key);
	cmp = compare(fromList1->key,fromList2->key);
	//printf("compare is %d\n",cmp);
	if(cmp>0){
	    //printf("%s goes after %s\n",fromList1->key,fromList2->key);
	    addToTail(dest,createNode(createKeyValue(fromList2->key,fromList2->val)));
	    listSize++;
	    free(fromList2);
	    fromList2=NULL;
	    fromList2 = popFromHead(list2);

	}
	else{
            //printf("cmp is less than 0\n");
	    //printf("%s goes after %s\n",fromList2->key,fromList1->key);
	    addToTail(dest,createNode(createKeyValue(fromList1->key,fromList1->val)));
	    listSize++;
	    fromList1=NULL;
	    free(fromList1);
	    fromList1 = popFromHead(list1);
	}
        //printf("current merged list:\n");
        //printLinkedList(dest);

    }
    if(fromList1 ==NULL){
	//printf("fromList1 is NULL so getting the next key\n");
	fromList1=popFromHead(list1);
    }
    if(fromList2 ==NULL){
	//printf("fromList2 is NULL so getting the next key\n");
	fromList2=popFromHead(list2);
    }

    //printf("adding the rest of the lists\n");
    while(fromList1 != NULL){
	addToTail(dest,createNode(createKeyValue(fromList1->key,fromList1->val)));
	listSize++;
        free(fromList1);
	fromList1 = popFromHead(list1);
    }
    //printf("finished adding list1\n");
    while(fromList2){
	addToTail(dest,createNode(createKeyValue(fromList2->key,fromList2->val)));
	listSize++;
        free(fromList2);
	fromList2 = popFromHead(list2);
    }
    return listSize;
}

void recursiveDeleteNode(LLNode *cur){
    if(cur){
	free(cur->kv->key);
	free(cur->kv);
	recursiveDeleteNode(cur->next);
	free(cur);
    }
}

void deleteLinkedList(LinkedList *list){
    recursiveDeleteNode(list->head);
    list->head=NULL;
}
//////////////////////////


/*Given a path to a file, returns the number of lines in that file*/

int lineCount(const char *path){
    /*pointer to the system call*/
    FILE *fp;
    int i,line_count;
    /*buffer for the results of the system call*/
    char buffer[100];
    /*This is the linux command to get the linecount. we need to append the file name*/
    char oldCommand[] = "wc -l ";
    /*create a string sizeof(oldcommand)+sizeof(file_path)*/
    char *command = (char*)malloc(sizeof(char)*(strlen(path)+strlen(oldCommand)+1));
    /*keep a pointer to the start of the new command*/
    char *command_start = command;
    
    /*create the new command*/
    for(i=0;i<(int)strlen(oldCommand);i++){
	*(command)=oldCommand[i];
	command++;
    }
    for(i=0;i<(int)strlen(path);i++){
	*(command)=path[i];
	command++;
    }
    //printf("System call: %s\n",command_start);
    
    /*call the command and store result in buffer*/
    fp = popen(command_start,"r");
    if(fp<0)
	return -1;

    /*WHY CANT I JUST DO FSCANF()???*/
    fgets(buffer,100,fp);
    /*scan for the line count and return*/
    sscanf(buffer,"%d",&line_count);
    /*clean up*/
    free(command_start);
    fclose(fp);
    return line_count;

} 

/*Given an input file path,line_count, thread_count, and thread_id return a FILE Pointer
 *to the start of a chunk of the document.
 *The chunk size is calculated by line_count/thread_count
 *The file points to the start line by 
 *4 threads:
 *    thread 0 = line_count*(0/4) +1 -> line_count*(1/4)
 *    thread 1 = line_count*(1/4) +1 -> line_count*(2/4)
 *    thread 2 = line_count*(2/4) +1 -> line_count*(3/4)
 *    thread 3 = line_count*(3/4) +1 -> line_count*(4/4)
 *  ==> i = 0,1,2,....
 *      thread[i]:
 *	   start = line_count*(i/thread_count) +1
 *	   finish = line_count*((i+1)/thread_count)
 */
FILE *getChunk(char *path, int line_count, int thread_count, int tid){
    FILE *fp;
    fp = NULL;
    int i;
    double intermediate;
    char startString[30];
    char finishString[30];
    char *command_start;
    char oldCommand[] = "sed -n \'";
  
    /*calculate start line*/
    intermediate = (double)(tid)/thread_count;
    int start = (int)(line_count*intermediate)+1;

    /*calculate finish line*/
    intermediate = (double)(tid+1)/thread_count;
    int finish = (int)line_count*intermediate;


    /*put the start/finish values into string form*/
    sprintf(startString,"%d",start);
    sprintf(finishString,"%d",finish);
        
    /*malloc size for the whole command: "sed -n '<start>,<finish>p' <file>"
     *the +3 is to includee the ,p'<space> in the string
     */
    
    char *command = (char*)malloc(sizeof(char)*(strlen(path)+strlen(oldCommand)+strlen(startString)
			+strlen(finishString)+6));
    /*keep track of the beginning of the string*/
    command_start=command;

    /*create the new command*/
    for(i=0;i<strlen(oldCommand);i++){
	*(command)=oldCommand[i];
	command++;
    }

    for(i=0;i<strlen(startString);i++){
	*(command)=startString[i];
	command++;
    }
    *(command)=',';
    command++;
    for(i=0;i<strlen(finishString);i++){
	*(command)=finishString[i];
	command++;
    }
    *(command)='p';
    command++;
    *(command)='\''; //this is the ascii code for a single quote
    command++;
    *(command)=' ';
    command++;
   for(i=0;i<strlen(path);i++){
	*(command)=path[i];
	command++;
    }
	
	/*For debugging: prints the command "sed -n..."*/
    //printf("System call: %s\n",command_start);

    /*use popen() to get a FILE pointer to the results of the sed call*/
    fp = popen(command_start,"r");

    /*free the allocated memory now that were done with it*/
    free(command_start);

    return fp;
}

int sedWC(char *path,char type){
    /*pointer to the system call*/
    FILE *fp;
    int i,byte_count;
    /*buffer for the results of the system call*/
    char buffer[100];
    /*This is the linux command to get the linecount. we need to append the file name*/
    char oldCommand[] = " wc -";
    /*create a string sizeof(oldcommand)+sizeof(file_path)*/
    char command[200];
    /*keep a pointer to the start of the new command*/
    //char *command_start = command;
    int j=0;
    /*create the new command*/
    for(i=0;i<strlen(path);i++){
	command[j]=path[i];
	j++;
    }
    for(i=0;i<strlen(oldCommand);i++){
	command[j]=oldCommand[i];
	j++;
    }
    command[j]=type;
    j++;
    command[j]='\0';
    //printf("System call: %s\n",command);
    
    /*call the command and store result in buffer*/
    fp = popen(command,"r");
    if(fp<0)
	return -1;

    /*WHY CANT I JUST DO FSCANF()???*/
    fgets(buffer,100,fp);
    /*scan for the line count and return*/
    sscanf(buffer,"%d",&byte_count);
    /*clean up*/
    //free(command_start);
    fclose(fp);
    return byte_count;

} 

char *getChunkCommand(char *path, int line_count, int thread_count, int tid){
    //FILE *fp;
    //fp = NULL;
    int i;
    double intermediate;
    char startString[30];
    char finishString[30];
    char *command_start;
    char oldCommand[] = "sed -n \'";
  
    /*calculate start line*/
    intermediate = (double)(tid)/thread_count;
    int start = (int)(line_count*intermediate)+1;

    /*calculate finish line*/
    intermediate = (double)(tid+1)/thread_count;
    int finish = (int)line_count*intermediate;


    /*put the start/finish values into string form*/
    sprintf(startString,"%d",start);
    sprintf(finishString,"%d",finish);
        
    /*malloc size for the whole command: "sed -n '<start>,<finish>p' <file>"
     *the +3 is to includee the ,p'<space> in the string
     */
    
    char *command = (char*)malloc(sizeof(char)*(strlen(path)+strlen(oldCommand)+strlen(startString)
			+strlen(finishString)+9));
    /*keep track of the beginning of the string*/
    command_start=command;

    /*create the new command*/
    for(i=0;i<strlen(oldCommand);i++){
	*(command)=oldCommand[i];
	command++;
    }

    for(i=0;i<strlen(startString);i++){
	*(command)=startString[i];
	command++;
    }
    *(command)=',';
    command++;
    for(i=0;i<strlen(finishString);i++){
	*(command)=finishString[i];
	command++;
    }
    *(command)='p';
    command++;
    *(command)='\''; //this is the ascii code for a single quote
    command++;
    *(command)=' ';
    command++;
   for(i=0;i<strlen(path);i++){
	*(command)=path[i];
	command++;
    }
    *(command)=' ';
    command++;
    *(command)='|';
    command++;
    *(command)='\0';
	/*For debugging: prints the command "sed -n..."*/
    
    //printf("System call: %s\n",command_start);
    //printf("sed command is %d in length\n",strlen(command_start));
    return command_start;
}

int getChunkBytes(char *path, int line_count, int thread_count, int tid){
    int bytes = 0;
    char *sed = getChunkCommand(path,line_count,thread_count,tid);
    bytes+=sedWC(sed,'c');
    bytes+=sedWC(sed,'w');
    free(sed);
    bytes=bytes-1;
    return bytes;

}

LinkedList *createProcLL(char mastermem[8],char *file,int thread_count,int line_count){
    int byte_size;
    int shm1,i,k;
    LinkedList *list = createLinkedList();
   // printf("before loop\n");
    for(i=0;i<thread_count;i++){
	mastermem[6]=(char)(i+65);
	//printf("using %s\n",mastermem);
	/*open shared memory*/
	shm1 = shm_open(mastermem, O_RDWR, 0777);
	/*get the number of bytes in the chunk*/
	byte_size = getChunkBytes(file,line_count,thread_count,i);
	//printf("size of chunk is %d\n",byte_size);
	/*allocate number of bytes in chunk on heap and read in shared memory*/
	char *mapped = (char*)malloc(sizeof(char)*byte_size+40);
	int myresult = read(shm1, mapped, byte_size);
	//printf("allocated map memory\n");
	k=0;
	char *newkey;
	//int l;
	//l=0;
        int m;
	int n=0;
	int prev_space=0;
	/*read each character and add to LinkedList whenever a space is found*/
	while(k<byte_size){
	    if(mapped[k]=='@')
		break;
	    //printf("%c",mapped[k]);
	    if(mapped[k]=='\0'){
		newkey = (char*)malloc(sizeof(char)*(k-prev_space)+2);
		for(m=prev_space;m<k;m++){
		    if(mapped[m]!=' ')
		    newkey[n]=mapped[m];
		    //printf("%c",newkey[n]);
		    n++;
		}
		newkey[n-1]=0;
		n=0;
		if (strcmpi(newkey," ")!=0)
			addToHead(list,createNode(createMappedKeyValue(newkey,1)));
		free(newkey);
		prev_space=k+1;
	    }
	    k++;
	}
	//printf("\n-------\n");
    }
    return list;
}

/* 
 *map the key and value and sort the LinkedList for each pthread
 *we just add to the head of the linked list and sort it later
 *also increment the global variable LISTSIZE so we know the number of words total in file
 */
void *mapKeyValue(void *args){
    LinkedList *List;
    KeyValue *kv;
    char str[100];
    FILE *fp;
    fp = args;
    //printf("***********New thread starting to map the file*************\n");
    List = createLinkedList();
    if (fp) {
	while (fscanf(fp, "%s", str)!=EOF){
	    //printf("%s ",str);
	    kv = createKeyValue(str,1);
	    addToHead(List,createNode(kv));
	}
    }
    //printf("finished creating all nodes\n");
    //printLinkedList(List);
    
    kv = NULL;
    pthread_exit(List);
}
/*for processes*/
void *mapKeyValue2(void *args){
    LinkedList *List;
    KeyValue *kv;
    char str[100];
    FILE *fp;
    fp = args;
    //printf("***********New thread starting to map the file*************\n");
    List = createLinkedList();
    if (fp) {
	while (fscanf(fp, "%s", str)!=EOF){
	    //printf("%s ",str);
	    kv = createKeyValue2(str,1);
	    addToHead(List,createNode(kv));
            //counter++;
	    //if((counter%10000)==0){
		//printf("created 10000 more nodes\n");
	    //}
	}
    }
    //printf("finished creating all nodes\n");
    //printLinkedList(List);
    
    kv = NULL;
    return (void*)List;
    //pthread_exit(List);
}
void writeWords(LinkedList *List, char *output){
    LLNode *cur = List->head;
    FILE *f = fopen(output, "w");
    if (f == NULL){
	printf("Error opening file!\n");
	exit(1);
    }
    while(cur!=NULL){
	fprintf(f,"%s %d\n",cur->kv->key,cur->kv->val);
	cur=cur->next;
    }
    
}
void writeNums(KeyValue **keys,int size,char *output){
    int i;
    FILE *f = fopen(output, "w");
    if (f == NULL){
	printf("Error opening file!\n");
	exit(1);
    }
    for(i=0;i<size;i++){
	fprintf(f,"%s\n",keys[i]->key);
    }
    
}
/* 
 *Reduce function: given a linked list of key values, this function will combine similar keys and increment the value
 *in this case we dont care about the COMPARE global, we just need to see if the two strings are equal, so use strcmpi()
 */
void *reduce(void *args){
    LinkedList *list;
    ThreadData *data;
    int start,end,cur;
    data = args;
    start = data->start;
    //printf("%s\n",final[start]->key);
    end = data->end;
    //printf("thread goes from %d->%d\n",start,end);
    list = createLinkedList();
    addToTail(list,createNode(createKeyValue(final[start]->key,final[start]->val)));
    free(final[start]);
    start++;
    for(cur=start;cur<end;cur++){
	if( (final[cur]->key[0] - list->tail->kv->key[0])== 0 && (strcmpi(final[cur]->key,list->tail->kv->key))==0)
	    list->tail->kv->val++;
	else
	    addToTail(list,createNode(createKeyValue(final[cur]->key,final[cur]->val)));
	free(final[cur]);
    }
    //printf("FINISHED WITH REDUCE\n");
    //printLinkedList(list);
    pthread_exit(list);
}

void reduceLinkedList(LinkedList *list){
    LLNode *cur,*next;
    cur = list->head;
    while(cur->next!=NULL){
        if((cur->kv->key[0] - cur->next->kv->key[0])==0 && strcmpi(cur->kv->key,cur->next->kv->key)==0){
	    /*we want to delete cur->next, increment cur->kv->val, set cur->next=next->next*/
	    next = cur->next;
	    cur->next=next->next;
	    free(next->kv->key);
	    free(next->kv);
	    free(next);
	    cur->kv->val++;
  	}
        else{
	    cur=cur->next;
	}
     }
}

/*given a linked list, its size, and the number of reduces, return an array of linkedlist
 *pointers splitting the original list into appropriate chunks
 */
LinkedList **splitList(LinkedList* original, int size, int num_reduce){
    LinkedList **ListArray;
    ListArray = (LinkedList**)malloc(sizeof(LinkedList*)*num_reduce);
    double intermediate;
    int i,j,start,finish;
    //LLNode *cur;
    KeyValue *kv; 
    for(i=0;i<num_reduce;i++){
	intermediate = (double)(i)/num_reduce; 
	start = (int)(size*intermediate);
	intermediate = (double)(i+1)/num_reduce;
        finish = (int)size*intermediate;
	ListArray[i] = createLinkedList();
        //printf("List[%d] has start=%d, finish=%d\n",i,start,finish);
	for(j=start;j<finish;j++){
	    kv = popFromHead(original);
	    addToTail(ListArray[i],createNode(createKeyValue(kv->key,kv->val)));
	    free(kv);
	}
	
    }
    return ListArray;
   
}


/*This is a comparison function i had to write in order to use qsort()
 *it takes in void *'s because qsort() needs to handle any kind of data
 *we need to also create a compareNums() for the integer sort
 *the other problem is if i use strcmpi(A,a) it returns 0
 *but if i use strcmp() then it thinks "Aani" goes before "a"
 *i think i need to create a new strcmp() function to solve this bug
 */
int compareWords(const void *p1, const void *p2){
    /*extract the KeyValue's keys from input*/
    char *key1=(*(KeyValue **)p1)->key;
    char *key2=(*(KeyValue **)p2)->key;
    int cmp;
    /*compare the two keys and return appropriate value*/
    cmp = strcmpi(key1,key2);
    if(cmp<0)
	return -1;
    else if(cmp>0)
	return 1;
    else{
	    if((key1[0]-key2[0])<0)
		return -1;
	    else if((key1[0]-key2[0])>0)
		return 1;
	    else
	        return 0;
	}
}

int compareNums(const void *p1, const void *p2){
    /*extract the KeyValue's keys from input*/
    char *key1=(*(KeyValue **)p1)->key;
    char *key2=(*(KeyValue **)p2)->key;
    int cmp;
    /*compare the two keys and return appropriate value*/
    cmp = numcmp(key1,key2);
    if(cmp<0)
	return -1;
    else if(cmp>0)
	return 1;
    else{
	    if((key1[0]-key2[0])<0)
		return -1;
	    else if((key1[0]-key2[0])>0)
		return 1;
	    else
	        return 0;
	}
}

/*this is used to determine the proper shared memory to place a word in*/
int placeWordInBin(KeyValue *key){
    if(key->key[0]>='a' && key->key[0]<='h'){
	return 1;
    }
    else if(key->key[0]>='i' && key->key[0]<='p'){
	return 2;
    }
    else if(key->key[0]>='q' && key->key[0]<='z'){
	return 3;
    }
    else if(key->key[0]>='A' && key->key[0]<='H'){
	return 1;
    }
    else if(key->key[0]>='I' && key->key[0]<='P'){
	return 2;
    }
    else if(key->key[0]>='Q' && key->key[0]<='Z'){
	return 3;
    }
    else{
	printf("ERROR this isnt working right\n");	
	return 0;
    }
}
/*this is used to determine the proper shared memory to place a number in*/
int placeNumInBin(KeyValue *key){
    int num = atoi(key->key);
    if(num>=0 && num<=715827882){
	return 1;
    }
    else if(num>=715827883 && num<=1431655764){
	return 2;
    }
    else if(num>=1431655764 && num<=2147483647){
	return 3;
    }
    else{
	printf("ERROR this isnt working right\n");	
	return 0;
    }
}
/*Given an unsorted LinkedList of Size size, the function returns a sorted
 *array of KeyValues. It uses the built in qsort() to sort the array
 *right now it writes the sorted array to a file but thats just for debugging
 *eventually ill have it also take in a pointer to the compare function
 *i want to pass to the qsort()
 */
KeyValue **sortKeyValues(LinkedList *Global,int size,int (*compare)(const void *,const void *)){
    KeyValue **array;
    int i;
    //int mycount=0;
    /*create array and populate with unsorted linked list*/
    array=(KeyValue**)malloc(sizeof(KeyValue*)*size);
    for(i=0;i<size;i++){
	array[i]=popFromHead(Global);
    }
    /*delete the linked list to possibly prevent memoryleaks*/
    deleteLinkedList(Global);
    /*sort the array*/
    qsort(array,size,sizeof(KeyValue*),compare);
    return array;    
}


void combineReducedLists(LinkedList *global, LinkedList *list){
    KeyValue *kv;
    if(global->tail!=NULL){
	/*if the tail of list1 is the same as tail from list 2*/
	if((global->tail->kv->key[0]-list->head->kv->key[0])==0)
	    if(strcmpi(global->tail->kv->key,list->head->kv->key)==0){
		//printf("%s AND %s ARE THE SAME, COMBINING KEYVALS\n",global->tail->kv->key,list->head->kv->key);		
		/*then combine their key->vals*/
		//printf("old tails was %d\n",global->tail->kv->val);
		global->tail->kv->val+=list->head->kv->val;
		/*remove the old head*/		
		kv = popFromHead(list);
		free(kv);
	    }
	if(list->head!=NULL){
	    global->tail->next = list->head;
	    global->tail = list->tail;
	}	
    }
    /*this is only when the list is empty*/
    else{
	global->head = list->head;
	global->tail = list->tail;
    }
}

/*adds the head of LinkedList list to the tail of LinkedList global*/
void combineLists(LinkedList* global, LinkedList *list){
    if(global->tail!=NULL){
	global->tail->next = list->head;
	global->tail = list->tail;
    }
    /*this is only when the list is empty*/
    else{
	global->head = list->head;
        global->tail=list->tail;
    }
}

ThreadData *createThreadData(int i,int num_red, int size){
    ThreadData *data=  (ThreadData*)malloc(sizeof(ThreadData));
    double intermediate;
    intermediate = (double)(i)/num_red;
    data->start =  (int)size*intermediate;
    intermediate = (double)(i+1)/num_red;
    data->end= (int)size*intermediate;
    return data;
}

void writeLinkedList(LinkedList *List, char *path){
    LLNode *cur = List->head;
    FILE *f = fopen(path, "w");
    if (f == NULL){
	printf("Error opening file!\n");
	exit(1);
    }
    while(cur!=NULL){
	fprintf(f,"%s\n",cur->kv->key);
	cur=cur->next;
    }
    
}


