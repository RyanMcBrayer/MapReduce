#include "stdio.h"
#include "stddef.h"
#include "stdlib.h"
#include "string.h"
#include "hashmap.h"
#include "pthread.h"
#include "mapreduce.h"
#include "errno.h"

//HashMap* hMap;
HashMap *maps[100];
struct kv_list *fakeHashMaps[100];
pthread_mutex_t mapLock[100];

int numMaps = 0;
Partitioner partitionFunc;
int numRed;


struct kv {
    char* key;
    char* value;
};

struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};

struct getterParams {
	Getter getFunc;
	Reducer reduceFunc;
	int partNum;
};

//struct kv_list kvl;
//size_t kvl_counter = 0;

size_t *kvlCounters;
char  returnWord[20];

struct kv_list* init_kv_list(size_t size) {
    
    struct kv_list *newList = (struct kv_list*)malloc(sizeof(struct kv_list));
	newList->elements = (struct kv**) malloc(size * sizeof(struct kv*));
    newList->num_elements = 0;
    newList->size = size;
	return newList;
}

void add_to_list(struct kv_list *list, struct kv* elt) {
    if (list->num_elements == list->size) {
	  list->size *= 2;
	  list->elements = realloc(list->elements, list->size * sizeof(struct kv*));
    }
    list->elements[list->num_elements++] = elt;
}


void printKvList(struct kv_list *list) {
   if (list == NULL) {
	   //printf("LIST IS NULL, CANNOT PRINT\n");
   }
   
   //printf("List size: %ld\n", list->size);
   for (int i = 0; i < list->size; i++) {
	 if (list->elements[i] != NULL) {
       //printf("\tKEY: %s, Value %s\n", list->elements[i]->key, (char*)list->elements[i]->value);
     }
   }


}

char* get_func(char* key, int partition_number) {

    struct kv_list *list = fakeHashMaps[partition_number];
    
	if (kvlCounters[partition_number] == list->num_elements) {
		return NULL;
    }
    //printf("indexing list->elements[] of %ld with size: %ld\n", kvlCounters[partition_number], list->num_elements);
	struct kv *curr_elt = list->elements[kvlCounters[partition_number]];
   
    //printf("Parameter key: %s", key);
	//printf("curr element key: %s\n", curr_elt->key);
	
	if (strcmp(curr_elt->key, key) == 0) {
		kvlCounters[partition_number]++;
		return curr_elt->value;
    }

    return NULL;

}

int cmp(const void* a, const void* b) {
    char* str1 = (*(struct kv **)a)->key;
    char* str2 = (*(struct kv **)b)->key;
    return strcmp(str1, str2);
}


int cmpstr(const void* a, const void* b)
{
    const char* aa = *(const char**)a;
    const char* bb = *(const char**)b;
    return strcmp(aa,bb);
}

void reduceWrapper(struct getterParams *params) {
	
	//printf("Parition number inside reduceWrapper is: %d\n", params->partNum);
	Reducer red = params->reduceFunc;
    pthread_mutex_lock(&mapLock[params->partNum]); 
	struct kv_list *partition = fakeHashMaps[params->partNum];
	char* prevKey = " ";
        int keysFound = 0;
	for(int i=0; i< partition->num_elements;i++){
                //char *newKey = strdup(partition->elements[i]->key);
		  //printf("prevKey: %s, currentKey: %s in partition: %d\n", prevKey, partition->elements[i]->key, params->partNum);
		if (strcmp(partition->elements[i]->key, prevKey) == 0) {
		  continue;
		}
                keysFound++;
                //printf("KEY FOUND: %s, total: %d\n", partition->elements[i]->key, keysFound);
		(*red)(partition->elements[i]->key, params->getFunc, params->partNum);
	    prevKey = partition->elements[i]->key;
	    //prevKey = strcpy(prevKey, partition->elements[i]->key);
	}
         //printf("Starting to close wrapper\n");
	kvlCounters[params->partNum] = 0;
    pthread_mutex_unlock(&mapLock[params->partNum]);
       //printf("Wrapper closed\n");
}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    
    //printf("Inside partition function with key: %s\n", key);
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}



void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, Partitioner partition) {   
    
    //INITIALIZING DUMMY HASH MAPS
    for (int i = 0; i < num_reducers; i++) {
      fakeHashMaps[i] = init_kv_list(10);
      int lockRet = pthread_mutex_init(&mapLock[i], NULL);
      if (lockRet != 0) {
		  //printf("lock at start of MR_Run failed\n");
		}
	}
	if (partition == NULL) {
		partitionFunc = &MR_DefaultHashPartition;
	} else {
		partitionFunc = partition;
    }
	numRed = num_reducers;  

    //printf("About to create threads\n");

    int numFiles = argc - 1;
    pthread_t mapThreads[num_mappers];
	for (int l = 0; l < num_mappers; l++) {
		mapThreads[l] = -1;
	}

	for(int i=0; i<argc; i++){ 
		//printf("INPUT ARG %d, value: %s\n", i, argv[i]);
    } 
	//CREATING MAP THREADS

    if(numFiles == num_mappers || numFiles < num_mappers) {
        //Create 1 mapper thread per file
    	for (int i = 1; i < argc; i++) {
			if (strstr(argv[i], ".") == NULL) {
				continue;
			 }
       	  pthread_t thread_id;
		  //printf("Filename: %s\n", argv[i]);
       	  pthread_create(&thread_id, NULL, (void *)(*map), argv[i]);
          //printf("THREAD CREATED \n");	
       	  mapThreads[i-1] = thread_id;
    	}	

    	//for(int j = 0; j < argc -1;j++){
      	for (int j = 0; j < num_mappers; j++) {
			if (mapThreads[j] != -1) {
				pthread_join(mapThreads[j], NULL);
			}
		}
     }
     else{
       //More files than mappers
       int fileIndex = 1;
       int fileCounter = numFiles;
       while(fileCounter > 0) {
         int loopCond = 0;
         if(fileCounter >= num_mappers) {
			loopCond = num_mappers;
         }
         else{
           loopCond = fileCounter;
         }
         
         for(int i=0; i< loopCond;i++){
             pthread_t thread_id;
			 if (strstr(argv[fileIndex], ".") == NULL) {
				fileIndex++;
				continue;
			 }
			//printf("Filename: %s\n", argv[fileIndex]);
             pthread_create(&thread_id, NULL, (void *)(*map), argv[fileIndex]);
             fileIndex++;
             //printf("THREAD CREATED\n");
             mapThreads[i] = thread_id;
         }
       
         for(int j=0;j< loopCond;j++){
             
			 if (mapThreads[j] != -1) {
				pthread_join(mapThreads[j], NULL);
             }
			 //printf("THREAD JOINED\n");
         }
         fileCounter -= num_mappers;
       }
     } 	

    //SORTING THE FAKE HASH MAPS
    for (int j = 0; j < num_reducers; j++) {
      
	  struct kv_list *curr_kv = fakeHashMaps[j]; 
      
	  if (curr_kv != NULL && curr_kv->num_elements > 0) {
        //printf("Partition: %d, Before sort...\n", j);
	    printKvList(curr_kv);
		qsort(curr_kv->elements, curr_kv->num_elements, sizeof(struct kv*), cmp);
        //printf("Partition: %d After sort...\n", j);
	    printKvList(curr_kv);
	  }

	}

    

	pthread_t reduceThreads[num_reducers];
    //kvl_counter = 0;

	//printf("Initializing kvlCounters\n");

	kvlCounters = malloc(sizeof(size_t) * num_reducers);

	for (int i = 0; i < num_reducers; i++) {
		kvlCounters[i] = 0;
	}
    //REDUCE DUMMY HASH MAPS
		
		int dumCount = 0;


		struct getterParams *structList[num_reducers];
                int structIndex[num_reducers];
                for(int i=0;i<num_reducers;i++)
                  structIndex[i] = -1;
		//struct getterParams *pa  = (struct getterParams *)malloc(sizeof(struct getterParams));
		
		for (int k = 0; k < num_reducers; k++) {
			if (fakeHashMaps[k] != NULL && fakeHashMaps[k]->num_elements > 0) {

				struct getterParams *pa  = (struct getterParams *)malloc(sizeof(struct getterParams));
				pa->getFunc = get_func;
				pa->reduceFunc = reduce;
				//pa->partNum = MR_DefaultHashPartition(fakeHashMaps[k]->elements[0]->key, numRed);
				pa->partNum = (*partitionFunc)(fakeHashMaps[k]->elements[0]->key, numRed);
				structList[k] = pa;
                                structIndex[k] = 1;
			}
		}
	
	for (int j = 0; j < num_reducers; j++) {
				
			pthread_mutex_lock(&mapLock[j]); 
			if (fakeHashMaps[j] != NULL && fakeHashMaps[j]->num_elements > 0) {
				//printf("IN iF STATEMENT\n");
			} else {
				//printf("IN else STATEMENT\n");
				continue;
				
			}
		
		    pthread_t thread_id2;
			int thread_ret = pthread_create(&thread_id2, NULL, (void *)(*reduceWrapper), structList[j]);
                        //printf("CREATING THREAD...\n");
                        if(thread_ret != 0)
                          printf("THREAD CREATE FAILED\n");
			reduceThreads[dumCount] = thread_id2;
			dumCount++;
			pthread_mutex_unlock(&mapLock[j]);
		}
	
	for(int j=0;j<dumCount;j++){
                //printf("JOINGING THREAD: %ld\n", reduceThreads[j]);
		pthread_join(reduceThreads[j], NULL);
	}


    //FREE
   free(kvlCounters); 

   for(int i=0;i<100;i++){
    if(fakeHashMaps[i] != NULL) {
     for(int j=0;j<fakeHashMaps[i]->num_elements;j++){
       free(fakeHashMaps[i]->elements[j]->key);
       free(fakeHashMaps[i]->elements[j]->value);
       free(fakeHashMaps[i]->elements[j]);
     }
     free(fakeHashMaps[i]->elements);
     free(fakeHashMaps[i]);
   }
  }
 

  for(int i=0;i<num_reducers;i++){
    if(structIndex[i] != -1){
      free(structList[i]);
    }
  }
  

}


void MR_Emit(char* key, char* value)
{
    struct kv *toAdd = (struct kv*) malloc(sizeof(struct kv));
    if (toAdd == NULL) {
	  //printf("Malloc error! %s\n", strerror(errno));
	  exit(1);
    }

	//int partitionNum = MR_DefaultHashPartition(key, numRed);
	int partitionNum = (*partitionFunc)(key, numRed);
    
    char* key2 = strdup(key);
	char* value2 = strdup(value);
	toAdd->key = key2;
    toAdd->value = value2;
	//printf("Adding pair to dummy map with partiotion num: %d  with key: %s and value: %s\n", partitionNum,  toAdd->key, (char*)toAdd->value);

    pthread_mutex_lock(&mapLock[partitionNum]); 
    add_to_list(fakeHashMaps[partitionNum], toAdd);
    pthread_mutex_unlock(&mapLock[partitionNum]);
	//free(key2);
	//free(value2);
	//free(toAdd);
	return;
}

