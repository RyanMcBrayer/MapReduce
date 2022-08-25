#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include "hashmap.h"
#include "pthread.h"

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL


HashMap* MapInit(void)
{
    HashMap* hashmap = (HashMap*) malloc(sizeof(HashMap));
    hashmap->contents = (MapPair**) calloc(MAP_INIT_CAPACITY, sizeof(MapPair*));
    hashmap->capacity = MAP_INIT_CAPACITY;
    hashmap->size = 0;

    pthread_rwlock_t  lock = PTHREAD_RWLOCK_INITIALIZER;

    rwlock = lock;

    return hashmap;
}

void printHash(HashMap *hmap) {

//printf("ACQUIRING READ LOCK IN printhash...\n");  
int lockret = pthread_rwlock_rdlock(&rwlock);
//printf("READ LOCK ACQUIRED IN printhash...\n");
    if (lockret != 0) {
     printf("printHash: lock acquire failed\n");
      //printf("Waiting in Map Put\n");
    }
 

  if (hmap->size > 0) {
    //printf("Contents of Current HashMap (size should be > 0):\n");
	 for (int i = 0; i < hmap->capacity; i++) {
		if (hmap->contents[i] != NULL) { 
			//printf("\tKey: %s, Value: %d\n", hmap->contents[i]->key, *(int*)hmap->contents[i]->value);
  		}
	}
  }
  //printf("reading contents\n"); 
  lockret = pthread_rwlock_unlock(&rwlock);
  //printf("READ LOCK RELEASED IN printhash\n");
}



void MapPut(HashMap* hashmap, char* key, void* value, int value_size)
{

    //printf("ACQUIRING WRITE LOCK IN MapPut...\n"); 
    int lockret = pthread_rwlock_wrlock(&rwlock);
    //printf("WRITE LOCK ACQUIRED IN MapPut...\n");
    if (lockret != 0) {
      //printf("MAP PUT: lock acquire failed\n");
      //printf("Waiting in Map Put\n");
    }
    

    if (hashmap->size > (hashmap->capacity / 2)) {
	if (resize_map(hashmap) < 0) {
            lockret = pthread_rwlock_unlock(&rwlock);
            printf("MAP PUT FAIL EXIT (LOCK RELEASED)\n");
	    exit(0);
	} 
    }
    
    MapPair* newpair = (MapPair*) malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = (void *)malloc(value_size);
    memcpy(newpair->value, value, value_size);
    h = Hash(key, hashmap->capacity);
    
	//printf("Putting key %s with value %d into hashmap with size of %ld\n", newpair->key, *(int*)newpair->value, hashmap->size);
    //printHash(hashmap); 

    while (hashmap->contents[h] != NULL) {
	// if keys are equal, update
	if (!strcmp(key, hashmap->contents[h]->key)) {
	    free(hashmap->contents[h]);
	    hashmap->contents[h] = newpair;
            lockret = pthread_rwlock_unlock(&rwlock);
            //printf("WRITE LOCK RELEASED IN MapPut\n"); 
	    return;
	}
	h++;
	if (h == hashmap->capacity)
	    h = 0;
    }

    // key not found in hashmap, h is an empty slot
    hashmap->contents[h] = newpair;
    hashmap->size += 1;
    
    //printf("Done Putting\n");
   lockret = pthread_rwlock_unlock(&rwlock);
   //printf("WRITE LOCK RELEASED IN MapPut\n"); 
}

char* MapGet(HashMap* hashmap, char* key)
{
    //printf("ACQUIRING READ LOCK IN MapGet...\n"); 
    int lockret = pthread_rwlock_rdlock(&rwlock);
    //printf("READ LOCK ACQUIRED IN MapGet...\n");

   
    if(lockret != 0) {
      //printf("MAP GET: lock acquire failed\n");
    }

    int h = Hash(key, hashmap->capacity);
    while (hashmap->contents[h] != NULL) {
	if (!strcmp(key, hashmap->contents[h]->key)) {
            lockret = pthread_rwlock_unlock(&rwlock);
            //printf("READ LOCK RELEASED IN MapGet\n");
	    return hashmap->contents[h]->value;
	}
	h++;
	if (h == hashmap->capacity) {
	    h = 0;
	}
    }

    lockret = pthread_rwlock_unlock(&rwlock);
    //printf("READ LOCK RELEASED IN MapGet\n");
    return NULL;
}

size_t MapSize(HashMap* map)
{

    //printf("ACQUIRING READ LOCK IN MapSize...\n"); 
    int lockret = pthread_rwlock_rdlock(&rwlock);
    //printf("READ LOCK ACQUIRED IN MapSize...\n");

      

    if(lockret != 0) {
      //printf("MAP SIZE: lock acquire failed\n");
    }

    int size = map->size;

    lockret = pthread_rwlock_unlock(&rwlock);
    //printf("READ LOCK RELEASED IN MapSize\n");
    return size;
}

int resize_map(HashMap* map)
{

    //printf("ACQUIRING WRITE LOCK IN resize_map...\n"); 
    //int lockret = pthread_rwlock_wrlock(&rwlock);
    //printf("WRITE LOCK ACQUIRED IN resize_map...\n");



    //if(lockret != 0) {
      //printf("RESIZE MAP: lock acquire failed\n");
    //}

    MapPair** temp;
    size_t newcapacity = map->capacity * 2; // double the capacity

    // allocate a new hashmap table
    temp = (MapPair**) calloc(newcapacity, sizeof(MapPair*));
    if (temp == NULL) {
	//printf("Malloc error! %s\n", strerror(errno));
        //lockret = pthread_rwlock_unlock(&rwlock);
        //printf("WRITE LOCK RELEASED IN resize_map\n");
	return -1;
    }

    size_t i;
    int h;
    MapPair* entry;
    // rehash all the old entries to fit the new table
    for (i = 0; i < map->capacity; i++) {
	if (map->contents[i] != NULL)
	    entry = map->contents[i];
	else 
	    continue;
	h = Hash(entry->key, newcapacity);
	while (temp[h] != NULL) {
	    h++;
	    if (h == newcapacity)
		h = 0;
	}
	temp[h] = entry;
    }

    // free the old table
    free(map->contents);
    // update contents with the new table, increase hashmap capacity
    map->contents = temp;
    map->capacity = newcapacity;

   //lockret = pthread_rwlock_unlock(&rwlock);
    //printf("WRITE LOCK RELEASED IN resize_map\n");
    return 0;
}

// FNV-1a hashing algorithm
// https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function#FNV-1a_hash
size_t Hash(char* key, size_t capacity) {
    size_t hash = FNV_OFFSET;
    for (const char *p = key; *p; p++) {
	hash ^= (size_t)(unsigned char)(*p);
	hash *= FNV_PRIME;
	hash ^= (size_t)(*p);
    }
    return (hash % capacity);
}

