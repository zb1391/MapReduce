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
#include "mapreduce.h"

/*GLOBAL POINTER TO THE COMPARE FUNCTION
 *This changes based on if wordcount/numsort*/

int (*COMPARE)(const void *,const void *);
int (*BINPLACE)(KeyValue *);
int LISTSIZE;
int countx;

int main(int argc, char **argv){
    /*gets the number of lines in a file*/
    FILE *file;  
    int i;
    /*THIS BUFFER IS VERY IMPORTANT! IF THERE IS A WORD MORE THAN 100 CHARACTERS IT BREAKS*/
    //char str[100];
    int line_count,thread_count,num_reduces;
    LinkedList *List,*GlobalList;
    GlobalList = createLinkedList();
    //tempList = createLinkedList();
    //List = createLinkedList();
    //KeyValue *kv;
    //LinkedList **ListArray;
    //int size = 0;
    //int fd;
    ARGS *args=(ARGS*)malloc(sizeof(ARGS));
    checkArgs(argc,argv,args);
    /*
    printf("num_maps is %d\n",args->num_maps);
    printf("num_reduces is %d\n",args->num_reduces);
    printf("input: %s\n",args->input);
    printf("output: %s\n",args->output);
    */
    if(args->sort_type == 0){
	printf("using compareWords\n");
	COMPARE=&compareWords;
	BINPLACE=&placeWordInBin;
    }
    else{
	printf("using compareNums\n");
	COMPARE=&compareNums;
	BINPLACE=&placeNumInBin;  
    }
    
    line_count = lineCount(args->input);
    
    printf("line count is %d\n",line_count);
   
    //TODO: ERROR CHECK FOR INVALID FILE PATH
    thread_count=args->num_maps;
    num_reduces=args->num_reduces;

/****************     		THREADS 		***************************/

    if(args->impl == 0 ){
	printf("THREADS\n");
	printf("Mapping Values in Threads\n");
	    pthread_t tids[thread_count];

	    /*start each thread*/
	    
	    for(i=0;i<thread_count;i++){
		file = getChunk(args->input,line_count,thread_count,i);
		printf("Beginning of the pthread create: %d\n",i);
		
		/* Create number of threads and pass the file segment to each thread to compose into key value pair */
		pthread_create(&tids[i],NULL,&mapKeyValue,(void *)file);
	    }
	     
	    /* wait for all thread to join before proceeding */
	    for(i=0;i<thread_count;i++){
		pthread_join(tids[i], (void**)&List);

		/*add thread's list to global list\*/
		combineLists(GlobalList,List);
	    }
	    printf("FINISHED WITH ALL THREADs\n");

	    LLNode* xxx = GlobalList->head;
	    countx=0;
	    /*gets the size of the global list*/
	    while(xxx){
		countx++;
		xxx=xxx->next;
	    }
	   
	    /*final is a global array of KeyValue pointers. Making it global was the easiest way for me
	     *to have each thread access the array
	     *sortKeyValues returns an array of sorted KeyValue *'s
	     */
	    final=sortKeyValues(GlobalList,countx,COMPARE);

	    if(args->sort_type==1){
		writeNums(final,countx,args->output);
		fclose(file);
		printf("done\n");
		return 0;
	    }

	    printf("Attempting reduce\n");
	    /*replace [2] with num_reduces*/
	    /*also make sure that num_reduces is > LISTSIZE*/
	    ThreadData *datas[num_reduces];
	    pthread_t reds[num_reduces];
	    LISTSIZE=countx;
	    for(i=0;i<num_reduces;i++){
		datas[i]=createThreadData(i,num_reduces,LISTSIZE);
		pthread_create(&reds[i],NULL,reduce,datas[i]);
	    }
	    LinkedList *tempList2;
	    LinkedList *finalList = createLinkedList();
	    for(i=0;i<num_reduces;i++){
		pthread_join(reds[i],(void **)&tempList2);
		printf("adding list[%d] to finalList\n",i);
		combineReducedLists(finalList,tempList2);
	    }
	    printf("Writing Final Reduced List\n");
	    //printLinkedList(finalList);
	    writeWords(finalList,args->output);
	    
	    printf("finished\n");

	  
	    //kv=NULL;
	    printf("trying to close file\n");
	    fclose(file);
	    printf("closed file\n");
	    return 0;
	    
    }
		

/*********************** 	PROCESSES  	*******************************************/

    //fd = shm_open("/myshm", O_CREAT | O_RDWR, S_IRUSR |S_IWUSR);
    //printf("Shm_open %d \n",fd);
    printf("Mapping with Processes\n");
    /* Start children */

    //forkChildren(thread_count);
  
    int j, nChildren;
    nChildren = thread_count;
    //int mysize=0;
    pid_t pid;
    for (j = 0; j < nChildren; j++){
	pid = fork();
	if (pid == -1) {
	    printf("Could not fork \n");
            return 0;	
	}
	if (pid == 0) {
	    char memah[8]="/memah ";
	    memah[6]=(char)(j+65);
	    char memip[8]="/memip ";
	    memip[6]=(char)(j+65);
	    char memqz[8]="/memqz ";
	    memqz[6]=(char)(j+65);
	    //char buf;
	    int fd1,fd2,fd3;
            fd1 = shm_open(memah, O_RDWR | O_CREAT, 0777);
            fd2 = shm_open(memip, O_RDWR | O_CREAT, 0777);
            fd3 = shm_open(memqz, O_RDWR | O_CREAT, 0777);
    	    if (fd1 < 0) {
		perror("Could not open shm:");
		return 0;
	    }
    	    if (fd2 < 0) {
		perror("Could not open shm:");
		return 0;
	    }
    	    if (fd3 < 0) {
		perror("Could not open shm:");
		return 0;
	    }
	    //ftruncate(fd1, 10000);
	    printf("I am a child: %d PID: %d\n",j,getpid());
	    file = getChunk(args->input,line_count,thread_count,j);
	    printf("Child %d got the chunk\n",j);
	    List = mapKeyValue2(file);
//	    //printLinkedList(List);
            LLNode* xxx = List->head;
	    int countx=0;
	    while(xxx){
		countx++;
		xxx=xxx->next;
	    }
	    //KeyValue **final = sortKeyValues(List,countx,COMPARE);
	    int bin;
	    KeyValue *kk;
	    while((kk=popFromHead(List))!=NULL){
		bin=BINPLACE(kk);
		//printf("looking to place %s\n",kk->key);
		if(bin==1)
		    write(fd1,kk->key,(strlen(kk->key)+1));
		else if(bin==2)
		    write(fd2,kk->key,(strlen(kk->key)+1));
		else if(bin==3)
		    write(fd3,kk->key,(strlen(kk->key)+1));
		else{
		    printf("ERROR: did not get placed in bin\n");
		 
		}
		//printf("wrote %s to sharedmemory\n",final[i]->key);
		free(kk);
	    }
            write(fd1,"@!",1);
	    write(fd1,NULL,1);
            write(fd2,"@!",2);
	    write(fd2,NULL,2);
            write(fd3,"@!",3);
	    write(fd3,NULL,3);
	    printf("proc %d has finished its code\n",j);
	    exit(0);
	}
    }
    
    
    int status, n;
    n = thread_count;
    //LLNode *buf[2];
    while (n > 0) {
    	pid = wait(&status);
	printf("Child with PID %ld exited with status 0x%x.\n", (long)pid,status);
	--n;
    }
    printf("reading from shared memory now\n");
    char mastermem1[8]="/memah ";
    char mastermem2[8]="/memip ";
    char mastermem3[8]="/memqz ";
    //int shm1;
    //int byte_size=0;
    //int k=0;
    int size1,size2,size3;
    /*get a list of all words a-h*/
    LinkedList *listah = createProcLL(mastermem1,args->input,thread_count,line_count);
    LLNode *x = listah->head;
    size1=0;size2=0;size3=0;
    while(x){
	size1++;
	x=x->next;
    }
    LinkedList *listip = createProcLL(mastermem2,args->input,thread_count,line_count);
    x = listip->head;
    while(x){
	size2++;
	x=x->next;
    }
    LinkedList *listqz = createProcLL(mastermem3,args->input,thread_count,line_count);
    x = listqz->head;
    while(x){
	size3++;
	x=x->next;
    }
    printf("Sorting key values\n");
    KeyValue **array1 = sortKeyValues(listah,size1,COMPARE);
    KeyValue **array2 = sortKeyValues(listip,size2,COMPARE);
    KeyValue **array3 = sortKeyValues(listqz,size3,COMPARE);
    printf("Reducing\n");
    /*convert array to list and merge into one large list*/
    listah = createLinkedList();
    listip = createLinkedList();
    listqz = createLinkedList();
    LinkedList *combined =createLinkedList();
    for(i=0;i<size1;i++){
	//printf("%s\n",array1[i]->key);
	addToTail(listah,createNode(createMappedKeyValue(array1[i]->key,1)));
	free(array1[i]);
    }
    //printLinkedList(listah);
    //printf("----\n");
    for(i=0;i<size2;i++){
	//printf("%s\n",array1[i]->key);
	addToTail(listip,createNode(createMappedKeyValue(array2[i]->key,1)));
	free(array2[i]);
    }
    //printLinkedList(listip);
    //printf("----\n");
    for(i=0;i<size3;i++){
	//printf("%s\n",array1[i]->key);
	addToTail(listqz,createNode(createMappedKeyValue(array3[i]->key,1)));
	free(array3[i]);
    }
    //printLinkedList(listqz);
    //printf("----\n");
    printf("combining linked lists\n");
    /*now i need to concatenate into one large linked list*/
    combineLists(combined,listah);
    combineLists(combined,listip);
    combineLists(combined,listqz);
    //printLinkedList(combined);
    printf("Writing to file\n");
    if(args->sort_type==1){
	writeLinkedList(combined,args->output);
	return 0;
    }
    reduceLinkedList(combined);
    writeWords(combined,args->output);
    for(i=0;i<thread_count;i++){
	mastermem1[6]=(char)(i+65);
	shm_unlink(mastermem1);
	mastermem2[6]=(char)(i+65);
	shm_unlink(mastermem2);
	mastermem3[6]=(char)(i+65);
	shm_unlink(mastermem3);
    }
    return 0;
    printf("trying to close file\n");
    fclose(file);
    printf("closed file\n");
    return 0;
}
