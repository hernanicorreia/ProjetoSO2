#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>
#include <unistd.h>

char req_fifo_path[MAX_PIPE_PATH_LENGTH];
char resp_fifo_path[MAX_PIPE_PATH_LENGTH];
char notif_fifo_path[MAX_PIPE_PATH_LENGTH];
char server_fifo_path[MAX_PIPE_PATH_LENGTH];
int req_fd;
int resp_fd;
int notif_fd;

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path) {
  
  strncpy(resp_fifo_path, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(notif_fifo_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(server_fifo_path, server_pipe_path, MAX_PIPE_PATH_LENGTH);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  unlink(req_pipe_path);
  mkfifo(req_pipe_path, 0666);
  mkfifo(resp_pipe_path, 0666);
  mkfifo(notif_pipe_path, 0666);
  if (access(req_pipe_path, F_OK) == -1 || access(resp_pipe_path, F_OK) == -1 || access(notif_pipe_path, F_OK) == -1) {
    return 1;
  }

  int server_fd;
  if ((server_fd = open(server_pipe_path, O_WRONLY)) == -1) {
    return 1;
  }
  memset(req_pipe_path + strlen(req_pipe_path), "\0", MAX_PIPE_PATH_LENGTH - strlen(req_pipe_path));
  memset(resp_pipe_path + strlen(resp_pipe_path), "\0", MAX_PIPE_PATH_LENGTH - strlen(resp_pipe_path));
  memset(notif_pipe_path + strlen(notif_pipe_path), "\0", MAX_PIPE_PATH_LENGTH - strlen(notif_pipe_path));  
  char* msg = OP_CODE_CONNECT;
  strncat(msg, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncat(msg, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncat(msg, notif_pipe_path, MAX_PIPE_PATH_LENGTH);
  
  write_all(server_fd, msg, 3*MAX_PIPE_PATH_LENGTH+1);
  close(server_fd);
  req_fd = open(req_pipe_path, O_RDWR);
  resp_fd = open(resp_pipe_path, O_RDWR);
  notif_fd = open(notif_pipe_path, O_RDWR);



  return 0;
}

int kvs_disconnect(void) {
  char* msg = OP_CODE_DISCONNECT;
  write_all(req_fd, msg, 1);
  char buffer[2];
  while(1){
    read_all(resp_fd, buffer, 2);
    if(buffer[0] == OP_CODE_DISCONNECT){
      break;
    }
  }
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  unlink(req_fifo_path);
  unlink(resp_fifo_path);
  unlink(notif_fifo_path);
  return 0;
}

int kvs_subscribe(const char *key) {
  char* msg = OP_CODE_SUBSCRIBE;
  strncat(msg, key, MAX_STRING_SIZE);
  memset(msg + strlen(msg), "\0", MAX_STRING_SIZE - strlen(msg));
  write_all(req_fd, msg, MAX_STRING_SIZE+1);
  return 0;
}

int kvs_unsubscribe(const char *key, const char *req_pipe_path) {
  char* msg = OP_CODE_UNSUBSCRIBE;
  strncat(msg, key, MAX_STRING_SIZE);
  memset(msg + strlen(msg), "\0", MAX_STRING_SIZE - strlen(msg));
  write_all(req_fd, msg, MAX_STRING_SIZE+1);
  return 0;
}
