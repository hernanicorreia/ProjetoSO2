#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>

#include "../common/constants.h"
#include "../common/protocol.h"
#include "../common/io.h"
#include "io.h"
#include "operations.h"
#include "parser.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct Client_Queue {
  char* req_path;
  char* resp_path;
  char* notif_path;
  struct Client_Queue *next;
} Client_Queue;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
sem_t sem_full;
sem_t sem_empty;


size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;
char reg_pipe[MAX_PIPE_PATH_LENGTH];
int all_fifos[MAX_SESSION_COUNT][3];


int flag = 0; // Declare flag as a global variable
Client_Queue* job_head = NULL; // Declare job_head as a global variable
int job_counter = 0; // Global counter for job index

Client_Queue* pop() {
  if (job_head == NULL) {
    return NULL;
  }
  Client_Queue* job = job_head;
  job_head = job_head->next;
  return job;
}

volatile sig_atomic_t sigusr1_received = 0;

// Signal handler for SIGUSR1
void handle_sigusr1(int sig) {
  if(sig == SIGUSR1){
    if(signal(SIGUSR1, handle_sigusr1) == SIG_ERR){
      exit(EXIT_FAILURE);
    }
    for(int i = 0; i < MAX_SESSION_COUNT; i++ ){
      close(all_fifos[i][0]);
      close(all_fifos[i][1]);
      close(all_fifos[i][2]);
    }
  }
}



void clear_subscriptions() {
    // Logic to clear all subscriptions in the hashtable
}

void close_fifos() {
    // Logic to close all FIFOs
}

void notify_clients_to_terminate() {
    // Logic to notify clients to terminate
}


int disconnect_client(int fifos[3]){
  kvs_unsubscribe_all(fifos[2]);
  if(close(fifos[0]) == -1){
    write_all(fifos[1], "21", 3);
    return 1;
    }
  if(close(fifos[2] == -1)){
    write_all(fifos[1], "21", 3);
    return 1;
    }
  write_all(fifos[1], "20", 3);
  close(fifos[1]);
  return 0;
}



//threads , hash os q tao sub


void* look_for_session(void* arg){
    
  (void)arg; //unused argument
  while(1){
    beginning_threads:
    sem_wait(&sem_empty);
    pthread_mutex_lock(&queue_lock); //create lock for the queue threads procura nao ha mais q um 
    if(flag == 1 && job_head == NULL){ //declare flag as a global variable
      pthread_mutex_unlock(&queue_lock);
      return NULL;
    }
    
    Client_Queue* job = pop(); //adapt the queue to work with the array(a função pop ia à lista e tirava de lá o primeiro elemento, agora podes só ter um counter global e dar return a esse indice do array de sessões, já que este nunca vai ficar"sem espaço")
    pthread_mutex_unlock(&queue_lock);
    
    
    //get the fifo paths and open all of them saving the fd's
    
    //fifo[0] = req_fifo_path, fifo[1] = resp_fifo_path, fifo[2] = notif_fifo_path
    strchr(job->req_path,'&')[0] = '\0';
    strchr(job->resp_path,'&')[0] = '\0';
    strchr(job->notif_path,'&')[0] = '\0';
    int req_fd = open(job->req_path, O_RDONLY);
    int resp_fd = open(job->resp_path, O_WRONLY);
    int notif_fd = open(job->notif_path, O_WRONLY);
    //save the fd's in the array of all fifos with a for loop that only writes if the position is not occupied
    for(int i = 0; i < MAX_SESSION_COUNT; i++){
      if(all_fifos[i][0] == -1){
        all_fifos[i][0] = req_fd;
        all_fifos[i][1] = resp_fd;
        all_fifos[i][2] = notif_fd;
        break;
      }
    }
    if(resp_fd == -1)
      return NULL;
    if(req_fd == -1 || notif_fd == -1){
      write(resp_fd, "11", 2);
      return NULL;
    }
    write(resp_fd, "10", 2);


    int client_fifos[3] = {req_fd, resp_fd, notif_fd};
    
    while(1){
      
      
      char buff;
      read_all(req_fd, &buff, 1, NULL);
      int op_code = atoi(&buff);
      switch(op_code){
        case 2:
          disconnect_client(client_fifos);
          sem_post(&sem_full);
          goto beginning_threads;
        case 3:
          //parse
          char key1[MAX_STRING_SIZE];
          int n1 = (int)read(req_fd, key1, MAX_STRING_SIZE);
          if(n1 == -1 || n1 == 1){
            disconnect_client( client_fifos);
            sem_post(&sem_full);
            goto beginning_threads;
          }
          if(subscribe_client_key(key1, client_fifos[2])){
            write(resp_fd, "30", 2);
          }
          else{
            write(resp_fd, "31", 2);
          }
          break;
        case 4:
          //parse
          char key2[MAX_STRING_SIZE]; 
          int n2 = (int)read(req_fd, key2, MAX_STRING_SIZE);
          if(n2 == -1 || n2 == 1){
            disconnect_client(client_fifos);
            sem_post(&sem_full);
            goto beginning_threads;
          }
          if(kvs_unsubscribe(key2, client_fifos[2])){
            write(resp_fd, "40", 2);
          }
          else{
            write(resp_fd, "41", 2);
          }
          break;
      }
      buff = '\0';
    }

  }
  return NULL;
}



int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
  pthread_t *session_threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  //BIG BOSS
  for(int i = 0; i < MAX_SESSION_COUNT; i++){
    pthread_create(&session_threads[i], NULL, look_for_session, NULL);
  }
  if(signal(SIGUSR1, handle_sigusr1) == SIG_ERR){
    exit(EXIT_FAILURE);
  }
  //O BIG BOSS tem de ir pondo a info das sessões no array de threads, e as threads vão lendo o pipe de requests e executando as funções correspondentes
  //Ele vai funcionar como o loop que metia os jobs na Client_Queue no nosso ultimo projeto, mas em vez de meter na queue, mete no array de threads, em principio é só fazer append ao array
  int reg_fd = open(reg_pipe, O_RDONLY);
  while(1){
    char op_code;
    read_all(reg_fd, &op_code, 1, NULL);
    if(op_code != '\0')
    if((atoi(&op_code)) == OP_CODE_CONNECT){
      Client_Queue* new_job = malloc(sizeof(Client_Queue));
      char tmp[MAX_PIPE_PATH_LENGTH];
      read_all(reg_fd, tmp, MAX_PIPE_PATH_LENGTH, NULL);
      new_job->req_path = strdup(tmp);
      read_all(reg_fd, tmp, MAX_PIPE_PATH_LENGTH, NULL);
      new_job->resp_path = strdup(tmp);
      read_all(reg_fd, tmp, MAX_PIPE_PATH_LENGTH, NULL);
      new_job->notif_path = strdup(tmp);
      new_job->next = NULL;
      sem_wait(&sem_full);
      pthread_mutex_lock(&queue_lock);
      if(job_head != NULL){
        Client_Queue* aux = job_head;
        new_job->next = aux;
      }
      job_head = new_job;
      pthread_mutex_unlock(&queue_lock);
      sem_post(&sem_empty);
    }
    op_code = '\0';
    
  }



  

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  for (unsigned int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (pthread_join(session_threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      free(session_threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int main(int argc, char **argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }



  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  strcat(reg_pipe, argv[4]);

  unlink(reg_pipe);

  mkfifo(reg_pipe, 0666);


  for(int i = 0; i < MAX_SESSION_COUNT; i++){
    all_fifos[i][0] = -1;
    all_fifos[i][1] = -1;
    all_fifos[i][2] = -1;
  }

  sem_init(&sem_full, 0, MAX_SESSION_COUNT);
  sem_init(&sem_empty, 0, 0);

  dispatch_threads(dir);

  
  


  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }


  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
