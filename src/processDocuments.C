#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <ctype.h>
#include <string>
#include <set>

using namespace std;

set<string> stop_words;

void readStopWords()
{
  FILE *f = fopen("radikal.stopWords", "r");
  if (!f) {
    printf("Unable to read stop word list: radikal.stopWords\n");
    exit(0);
  }
  char buf[100];
  while (!feof(f)) {
    fscanf(f, "%s ", buf);
    stop_words.insert(buf);
  }
};

bool stopWord (char* word)
{
  return (stop_words.find(string(word))!=stop_words.end());
}

void formFilteredChars(char *filteredChars)
{
  int current;
  int i;

  current = 0;
  for (i = 1; i < 256; i++) {
    if (!((char) i >= 'A' && (char) i <= 'Z') &&
        !((char) i >= 'a' && (char) i <= 'z') &&
        i != 39 &&
	i != 199 && i != 208 && i != 214 && 
	i != 220 && i != 221 && i != 222 &&
	i != 231 && i != 240 && i != 246 &&
	i != 252 && i != 253 && i != 254) {
      filteredChars[current] = (char) i;
      current++;
    }
  }
  filteredChars[current] = '\0';
  /*  printf("Filtered Chars are: %s\n", filteredChars);*/
}

void fixTurkishCharacters(char *word)
{
  int i;
  char chars[12] = {199, 208, 214, 220, 221, 222,
		    231, 240, 246, 252, 253, 254};

  for (i = 0; i < strlen(word); i++)
    if (word[i] == chars[0])
      word[i] = 'C';
    else if (word[i] == chars[1])
      word[i] = 'G';
    else if (word[i] == chars[2])
      word[i] = 'O';
    else if (word[i] == chars[3])
      word[i] = 'U';
    else if (word[i] == chars[4])
      word[i] = 'I';
    else if (word[i] == chars[5])
      word[i] = 'S';
    else if (word[i] == chars[6])
      word[i] = 'c';
    else if (word[i] == chars[7])
      word[i] = 'g';
    else if(word[i] == chars[8])
      word[i] = 'o';
    else if (word[i] == chars[9])
      word[i] = 'u';
    else if (word[i] == chars[10])
      word[i] = 'i';
    else if (word[i] == chars[11])
      word[i] = 's';
}

bool eliminateHTMLTag(char *word)
{
  char *htmlTags[50] = {"A", "B", "P", "H", "H1", "H2", "H3", "H4", "H5",
                        "H6", "BR", "FONT", "CENTER", "ALIGN", "ARIAL",
                        "WIDTH", "HEIGHT", "IMG", "SRC", "HSPACE",
                        "VSPACE", "BORDER", "SIZE", "FACE", "HREF", "HTML",
                        "HABERIN", "DEVAMI", "ICIN", "TIKLAYINIZ", "GIF",
                        "COURIER", "RESIM", "TABLE", "CELLSPACING",
                        "CELLPADDING", "LEFT", "RIGHT", "DD"};
  int i;

  for (i = 0; i < 39; i++)
    if (!strcmp(word, htmlTags[i]))
      return true;
  return false;
}

void convertToLowerCase(char *word)
{
  int i;

  for (i = 0; i < strlen(word); i++)
    word[i] = tolower(word[i]);
}

void convertToUpperCase(char *word)
{
  int i;

  for (i = 0; i < strlen(word); i++)
    word[i] = toupper(word[i]);
}

int main(int argc, char *argv[])
{
  FILE *infile, *outfile;
  DIR *categoryDir, *mainDir;
  struct dirent *document, *category;
  char buffer[5000], word[500], tmpstr[5000];
  char filteredChars[255];
  char *line, *token;
  int wordCount;

  if (argc < 2) {
    printf("Usage: processDocuments <class directory name>\n");
    exit(0);
  }
  formFilteredChars(filteredChars);
  categoryDir = opendir(argv[1]);
  document = (struct dirent *) calloc (1, sizeof(struct dirent));
  if (!categoryDir) {
    printf("Unable to open category directory.\n");
    exit(0);
  }
  readStopWords();
  /* make output dir */
  sprintf(tmpstr, "text/%s", argv[1]);
  mkdir ("text", S_IRUSR|S_IWUSR|S_IXUSR);
  mkdir (tmpstr, S_IRUSR|S_IWUSR|S_IXUSR);
  while (1) {
    document = readdir(categoryDir);
    if (!document)
      break;
    if (!strcmp(document->d_name, ".") || !strcmp(document->d_name, "..") ||
	document->d_type != 0) /* BUG: does not work with xfs file system */
      continue;

    sprintf(tmpstr, "text/%s/%s.txt", argv[1], document->d_name);
    outfile = fopen(tmpstr, "w");
    if (!outfile) {
      printf("Unable to open output file %s\n", tmpstr);
      exit(0);
    }
    sprintf(tmpstr, "%s/%s", argv[1], document->d_name);
    infile = fopen(tmpstr, "r");
    if (!infile) {
      printf("Unable to open document %s", tmpstr);
      exit(0);
    }
    wordCount = 0;
    while(fgets(buffer, 5000, infile)) {
      line = buffer;
      if ((line[0] == '<' || line[0] == '[') &&
	  (line[1] == '!' || line[1] == 'F' || line[1] == '/' ||
	   line[1] == 'A' || line[1] == 'a' || line[1] == 'f' ||
	   line[1] == '<'))
	continue;
      while (token = strsep(&line, filteredChars)) {
	if (!line)
	  break;
	if (strlen(token) > 1) {
          if (strchr(token, '\''))
            token[strchr(token, '\'')-token] = '\0';
	  strcpy(word, token);
	  fixTurkishCharacters(word);
  	  convertToUpperCase(word);
	  if (!eliminateHTMLTag(word) && !stopWord(word)) {
	    fprintf(outfile, "%s ", word);
	    wordCount++;
	    if (wordCount == 7) {
	      fprintf(outfile, "\n");
	      wordCount = 0;
	    }
	  }
	}
      }
    }
    if (wordCount)
      fprintf(outfile, "\n");
    fclose(outfile);
    fclose(infile);
  }
  closedir(categoryDir);

  return 1;
}
