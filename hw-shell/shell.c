#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#include "tokenizer.h"

/* Convenience macro to silence compiler warnings about unused function parameters. */
#define unused __attribute__((unused))

/* Whether the shell is connected to an actual terminal or not. */
bool shell_is_interactive;

/* File descriptor for the shell input */
int shell_terminal;

/* Terminal mode settings for the shell */
struct termios shell_tmodes;

/* Process group id for the shell */
pid_t shell_pgid;

int cmd_exit(struct tokens* tokens);
int cmd_help(struct tokens* tokens);
int cmd_pwd(struct tokens* tokens);
int cmd_cd(struct tokens* tokens);

/* Built-in command functions take token array (see parse.h) and return int */
typedef int cmd_fun_t(struct tokens* tokens);

/* Built-in command struct and lookup table */
typedef struct fun_desc {
  cmd_fun_t* fun;
  char* cmd;
  char* doc;
} fun_desc_t;

fun_desc_t cmd_table[] = {
    {cmd_help, "?", "show this help menu"},
    {cmd_exit, "exit", "exit the command shell"},
    {cmd_pwd, "pwd", "print the current working directory"},
    {cmd_cd, "cd", "change the current working directory"},
};

/* Prints a helpful description for the given command */
int cmd_help(unused struct tokens* tokens) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    printf("%s - %s\n", cmd_table[i].cmd, cmd_table[i].doc);
  return 1;
}

/* Exits this shell */
int cmd_exit(unused struct tokens* tokens) { exit(0); }

/*print the current working directory*/
int cmd_pwd(struct tokens* tokens){
  //pwd has no argument
  if(tokens_get_length(tokens)>1){
    fprintf(stderr,"too many argument\n");
    return -1;
  }

  char* buffer=NULL;
  //In default GNU, if buffer==NULL, 
  //getcwd would malloc space for the current path, return the space address
  buffer=getcwd(buffer,0);
  if(!buffer)
    buffer="could not print the current working directory";
  fprintf(stdout,"%s\n",buffer);
  return 1;
}

/*change the working directory to the argument*/
int cmd_cd(struct tokens* tokens){
  if(tokens_get_length(tokens)>2){
    fprintf(stderr,"too many argument\n");
    return -1;
  }
  //get the argument
  char *path=tokens_get_token(tokens,1);
  if(!path||chdir(path)<0){
    fprintf(stderr,"invalid argument\n");
    return -1;
  }
  return 1;
}

const int fd_max=10;

/*proceed redirection
update ARGV - set to NULL
return 0 if succeed - return -1 if invalid syntax, open fail, dup2 fail
*/
int redirectionCheck(int ARGC, char* ARGV[], int fd[]){
  int checkpoint1,checkpoint2;
  int fd_count=0;

  for(int i=0;i<ARGC;i++){
    //redirection
    checkpoint1=strcmp(ARGV[i],"<");
    checkpoint2=strcmp(ARGV[i],">");
    if(!checkpoint1||!checkpoint2){
      //at the last position: no file
      if(i==ARGC-1){
        fprintf(stderr,"invalid syntax of %s\n", ARGV[i]);
        return -1;
      }
      
      //open the file
      fd[fd_count]=open(ARGV[i+1],checkpoint1?O_WRONLY|O_CREAT:O_RDONLY);
      if(fd[fd_count]<0){
        fprintf(stderr,"open file %s failed\n", ARGV[i+1]);
        return -1;
      }

      //redirection
      if(dup2(fd[fd_count],checkpoint1?STDOUT_FILENO:STDIN_FILENO)<0){
        fprintf(stderr,"dup2 failed %s\n", ARGV[i+1]);
        return -1;
      }

      //info update
      fd_count++;
      ARGV[i]=NULL,ARGV[i+1]=NULL;
      i++;
    }  
  }//end of for loop

  return 0;
}

//exec new program
int programExec(char* ARGV[]){
  char *pathToken;
    char *newPath;
    char *originPath=ARGV[0];
    int originLength=strlen(originPath);
    /*environment variable*/
    char *source;
    char *PATH=getenv("PATH");
    if(PATH){
      source=(char *)calloc(1,strlen(PATH)+1);
      if(source)
        strcpy(source,PATH);
    }

    //first try: suppose ARGV[0] is full path name
    if(execv(originPath,ARGV)<0){
      while(source&&(pathToken=__strtok_r(source,":",&source))){
        newPath=(char *)calloc(1,strlen(pathToken)+originLength+2);
        if(!newPath){
          fprintf(stderr,"heap allocate fail\n");
          return -1;
        }
        strcpy(newPath,pathToken);
        strcat(newPath,"/");
        strcat(newPath,originPath);
        ARGV[0]=newPath;
        
        /*for test*/
        //fprintf(stdout,"%s\n",newPath);

        //succeed
        if(execv(newPath,ARGV)>=0){
          free(newPath);
          return 0;
        }

        //cleaning if fail
        free(newPath);
      }//end of while loop

      //after all, no success
      fprintf(stderr,"run program %s fail\n",originPath);
      return -1;
    }

    return 0;
}

//free memory
void cleaning(char *ARGV[], int fd[]){
  free(ARGV);
  for(int fd_count=0;fd[fd_count]!=-1;fd_count++){
    close(fd[fd_count]);
  }
}

/*run programs with given path if not build in commands*/
int run_program(struct tokens* tokens){
  int ARGC=tokens_get_length(tokens);
  //no path
  if(ARGC < 1)
    return -1;
  
  pid_t cpid=fork();
  //in parent process
  if(cpid>0){
    int status;
    wait(&status);
  }
  //in child process
  else if(cpid==0){
    char** ARGV=(char **)calloc(ARGC+1,sizeof(char *));
    if(!ARGV){
      fprintf(stderr,"heap allocate fail\n");
      exit(-1);
    }

    for(int i=0;i<ARGC;i++){
      //have checked tokens validation and i is always valid
      ARGV[i]=tokens_get_token(tokens,i);
    }
    
    int fd[10]={-1};

    if(redirectionCheck(ARGC,ARGV,fd)<0||programExec(ARGV)<0){
      //error info printed in the function
      cleaning(ARGV,fd);
      exit(-1);
    }
    else{
      cleaning(ARGV,fd);
      exit(1);
    }
    
  }
  //error
  else{
    fprintf(stderr,"fork fail\n");
    return -1;
  }
  return 1;
}

/* Looks up the built-in command, if it exists. */
int lookup(char cmd[]) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    if (cmd && (strcmp(cmd_table[i].cmd, cmd) == 0))
      return i;
  return -1;
}

/* Intialization procedures for this shell */
void init_shell() {
  /* Our shell is connected to standard input. */
  shell_terminal = STDIN_FILENO;

  /* Check if we are running interactively */
  shell_is_interactive = isatty(shell_terminal);

  if (shell_is_interactive) {
    /* If the shell is not currently in the foreground, we must pause the shell until it becomes a
     * foreground process. We use SIGTTIN to pause the shell. When the shell gets moved to the
     * foreground, we'll receive a SIGCONT. */
    while (tcgetpgrp(shell_terminal) != (shell_pgid = getpgrp()))
      kill(-shell_pgid, SIGTTIN);

    /* Saves the shell's process id */
    shell_pgid = getpid();

    /* Take control of the terminal */
    tcsetpgrp(shell_terminal, shell_pgid);

    /* Save the current termios to a variable, so it can be restored later. */
    tcgetattr(shell_terminal, &shell_tmodes);
  }
}

int main(unused int argc, unused char* argv[]) {
  init_shell();

  static char line[4096];
  int line_num = 0;

  /* Please only print shell prompts when standard input is not a tty */
  if (shell_is_interactive)
    fprintf(stdout, "%d: ", line_num);

  while (fgets(line, 4096, stdin)) {
    /* Split our line into words. */
    struct tokens* tokens = tokenize(line);

    /* Find which built-in function to run. */
    int fundex = lookup(tokens_get_token(tokens, 0));

    if (fundex >= 0) {
      cmd_table[fundex].fun(tokens);
    } else {
      /* REPLACE this to run commands as programs. */
      //fprintf(stdout, "This shell doesn't know how to run programs.\n");
      if(tokens)
        run_program(tokens);
    }

    if (shell_is_interactive)
      /* Please only print shell prompts when standard input is not a tty */
      fprintf(stdout, "%d: ", ++line_num);

    /* Clean up memory */
    tokens_destroy(tokens);
  }

  return 0;
}
