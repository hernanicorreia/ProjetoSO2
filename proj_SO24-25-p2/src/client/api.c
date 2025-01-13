#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>



char* req_fifo_path;
char* resp_fifo_path;
char* notif_fifo_path;
char* server_fifo_path;
int req_fd;
int resp_fd;
int notif_fd;

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path) {
  
  req_fifo_path = (char*)req_pipe_path;
  resp_fifo_path = (char*)resp_pipe_path;
  notif_fifo_path = (char*)notif_pipe_path;
  server_fifo_path = (char*)server_pipe_path;
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  unlink(req_pipe_path);
  mkfifo(req_pipe_path, 0666);
  mkfifo(resp_pipe_path, 0666);
  mkfifo(notif_pipe_path, 0666);
  

  int server_fd;
  if ((server_fd = open(server_pipe_path, O_WRONLY)) == -1) {
    return 1;
  }
  memset(req_fifo_path + strlen(req_fifo_path), '&', MAX_PIPE_PATH_LENGTH - strlen(req_fifo_path));
  memset(resp_fifo_path + strlen(resp_fifo_path), '&', MAX_PIPE_PATH_LENGTH - strlen(resp_fifo_path));
  memset(notif_fifo_path + strlen(notif_fifo_path), '&', MAX_PIPE_PATH_LENGTH - strlen(notif_fifo_path));  
  char msg[3 * MAX_PIPE_PATH_LENGTH + 1] = "1"; 
  strcat(msg, req_fifo_path);
  strcat(msg, resp_fifo_path);
  strcat(msg, notif_fifo_path);
  
  write_all(server_fd, msg, 3*MAX_PIPE_PATH_LENGTH+1);
  close(server_fd);
  strchr(req_fifo_path, '&')[0] = '\0';
  strchr(resp_fifo_path, '&')[0] = '\0';
  strchr(notif_fifo_path, '&')[0] = '\0';
  req_fd = open(req_fifo_path, O_WRONLY);
  resp_fd = open(resp_fifo_path, O_RDONLY);
  notif_fd = open(notif_fifo_path, O_RDONLY);
  await_response(resp_fd, OP_CODE_CONNECT);
  


  return notif_fd;
}

int kvs_disconnect(void) {
  char msg = '2';
  write_all(req_fd, &msg, 1);
  await_response(resp_fd, OP_CODE_DISCONNECT);
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  unlink(req_fifo_path);
  unlink(resp_fifo_path);
  unlink(notif_fifo_path);
  return 0;
}

int kvs_subscribe(const char *key) {
  char msg[1 + MAX_STRING_SIZE] = "3";
  strcat(msg, key);
  memset(msg + strlen(msg), '\0', MAX_STRING_SIZE - strlen(msg));
  write_all(req_fd, msg, MAX_STRING_SIZE+1);
  await_response(resp_fd, OP_CODE_SUBSCRIBE);
  return 0;
}

int kvs_unsubscribe(const char *key) {
  char msg[1 + MAX_STRING_SIZE] = "4";
  strcat(msg, key);
  memset(msg + strlen(msg), '\0', MAX_STRING_SIZE - strlen(msg));
  write_all(req_fd, msg, MAX_STRING_SIZE+1);
  await_response(resp_fd, OP_CODE_UNSUBSCRIBE);
  return 0;
}
