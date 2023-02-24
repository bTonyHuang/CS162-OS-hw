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

#include <sys/stat.h>

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
int cmd_wait(struct tokens* tokens);

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
    {cmd_wait,"wait","waits until all background jobs have terminated"}
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

/*waits until all background jobs have terminated before returning to the prompt*/
int cmd_wait(struct tokens* tokens){
  int status, pid;
  while ((pid = wait(&status))) {
    if (pid == -1) {
      break;
    }
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

/*
The shell should basically ignore most of these signals
while the child/subprocess should act in default
*/
#define SHELLSET 0
#define CHILDSET 1
int ignore_signals[] = {
  SIGINT, SIGQUIT, SIGTERM, SIGTSTP,
  SIGCONT, SIGTTIN, SIGTTOU
};

void sigaction_set(int type){
  struct sigaction Act;

  if(type==SHELLSET)
    Act.sa_handler = SIG_IGN;
  else if(type==CHILDSET)
    Act.sa_handler = SIG_DFL;
  else
    return;

  sigemptyset (&Act.sa_mask);
  Act.sa_flags = 0;
  
  for(int i=0;i<sizeof(ignore_signals)/sizeof(int);i++)
    sigaction(ignore_signals[i],&Act,NULL);

}

/* Intialization procedures for this shell */
void init_shell() {
  // The shell should basically ignore most of these signals
  sigaction_set(SHELLSET);

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


/*proceed redirection
update ARGV - set to NULL
return 0 if succeed - return -1 if invalid syntax, open fail, dup2 fail
*/
int redirectionCheck(char* ARGV[]){
  int checkpoint1,checkpoint2;
  int fd,fd_count=0;

  for(int i=0;ARGV[i]!=NULL;i++){
    //redirection
    checkpoint1=strcmp(ARGV[i],"<");
    checkpoint2=strcmp(ARGV[i],">");
    if(!checkpoint1||!checkpoint2){
      //at the last position: no file
      if(!ARGV[i+1]){
        fprintf(stderr,"invalid syntax of %s\n", ARGV[i]);
        return -1;
      }
      
      mode_t f_attrib = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;  
      //open the file
      fd=open(ARGV[i+1],checkpoint1?O_WRONLY|O_CREAT|O_TRUNC:O_RDONLY,f_attrib);
      if(fd<0){
        fprintf(stderr,"open file %s failed\n", ARGV[i+1]);
        return -1;
      }

      //redirection
      if(dup2(fd,checkpoint1?STDOUT_FILENO:STDIN_FILENO)<0){
        fprintf(stderr,"dup2 failed %s\n", ARGV[i+1]);
        return -1;
      }
      close(fd);

      //info update
      fd_count++;
      ARGV[i]=NULL,ARGV[i+1]=NULL;
      i++;
    }  
  }//end of for loop

  return 0;
}

int pipeCheck(char* ARGV[],int run_bg){
  //count pipe
  int pipeCount=0;
  for(int i=0;ARGV[i]!=NULL;i++){
    if(!strcmp(ARGV[i],"|")){
      //the last token is "|"
      if(!ARGV[i+1]){
        fprintf(stderr,"invalid syntax of |\n");
        return -1;
      }
      pipeCount++;
    }
  }
  if(!pipeCount)
    return 0;

  //record program names index
  int* programName=(int*)calloc(pipeCount,sizeof(int));
  if(!programName){
    fprintf(stderr,"heap allocate programName failed\n");
    return -1;
  }
  int nameCount=0;
  for(int i=0;ARGV[i]!=NULL;i++){
    if(!strcmp(ARGV[i],"|")){
      programName[nameCount++]=i+1;
      ARGV[i]=NULL;
    }
  }
  
  //allocate pipe_arr[2*pipeCount]
  int** pipe_arr=(int **)calloc(pipeCount,sizeof(int*));
  if(!pipe_arr){
    fprintf(stderr,"heap allocate pipe_arr failed\n");
    free(programName);
    return -1;
  }
  for(int i=0;i<pipeCount;i++){
    pipe_arr[i]=(int *)calloc(2,sizeof(int));
    pipe(pipe_arr[i]);
  }

  pid_t cpid;
  //fork pipeCount process
  int i;
  for(i=0;i<pipeCount;i++){
    cpid=fork();
    //parent process
    if(cpid>0){
      continue;
    }
    //child process
    else if(!cpid){
      //set signal
      /*child process should respond with default action*/
      setpgid(0,0);
      if(!run_bg)
        tcsetpgrp(shell_terminal, getpgrp());
      sigaction_set(CHILDSET);
      break;
    }
    //fork error
    else{
      fprintf(stderr,"fork fail\n");
      return -1;
    }
  }

  //parent process
  if(i==pipeCount){
    if(dup2(pipe_arr[0][1],STDOUT_FILENO)<0){
      fprintf(stderr,"dup2 failed in pipes\n");
      return -1;
    }
    for(int k=0;k<pipeCount;k++){
      close(pipe_arr[k][0]);
      close(pipe_arr[k][1]);
    }
  }
  //child process
  else{
    dup2(pipe_arr[i][0],STDIN_FILENO);
    if(i!=pipeCount-1){
      dup2(pipe_arr[i+1][1],STDOUT_FILENO);
    }

    for(int k=0;k<pipeCount;k++){
      close(pipe_arr[k][0]);
      close(pipe_arr[k][1]);
    }

    int j;
    for(j=programName[i];ARGV[j]!=NULL;j++){
      ARGV[j-programName[i]]=ARGV[j];
    }
    ARGV[j-programName[i]]=NULL;
  }

  //cleaning
  free(programName);
  free(pipe_arr);
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

/*run programs with given path if not build in commands*/
int run_program(struct tokens* tokens){
  //param invalidation
  if(!tokens)
    return -1;
  
  int ARGC=tokens_get_length(tokens);
  //no path
  if(ARGC < 1)
    return 0;
  
  int run_bg=0;
  if(!strcmp(tokens_get_token(tokens,ARGC-1),"&")){
    run_bg=1;
  }


  pid_t cpid=fork();
  //in parent process
  if(cpid>0){
    int status;
    wait(&status);
    //returning to shell foreground
    tcsetpgrp(shell_terminal, shell_pgid);
  }
  //in child process
  else if(cpid==0){
    //set signal
    /*child process should respond with default action*/
    setpgid(0,0);
    if(!run_bg)
      tcsetpgrp(shell_terminal, getpgrp());
    sigaction_set(CHILDSET);

    char** ARGV=(char **)calloc(ARGC+1,sizeof(char *));
    if(!ARGV){
      fprintf(stderr,"heap allocate fail\n");
      exit(-1);
    }

    for(int i=0;i<ARGC;i++){
      //have checked tokens validation and i is always valid
      ARGV[i]=tokens_get_token(tokens,i);
    }

    //eliminate "&"
    if(run_bg)
      ARGV[ARGC-1]=NULL;
    
    if(pipeCheck(ARGV,run_bg)<0||redirectionCheck(ARGV)<0||programExec(ARGV)<0){
      //error info printed in the function
      free(ARGV);
      exit(-1);
    }
    else{
      free(ARGV);
      exit(0);
    }

  }
  //error
  else{
    fprintf(stderr,"fork fail\n");
    return -1;
  }
  return 0;
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

      if(run_program(tokens)<0){
        fprintf(stderr,"run_program failed\n");
        tokens_destroy(tokens);
        return -1;
      }
    }

    if (shell_is_interactive)
      /* Please only print shell prompts when standard input is not a tty */
      fprintf(stdout, "%d: ", ++line_num);

    /* Clean up memory */
    tokens_destroy(tokens);
  }

  return 0;
}
