#include <stdio.h>
#include <iostream>
#include <curl/curl.h>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <chrono>
#include <ctime>
 
std::string primaryNode = "http://192.168.30.112:5000";
std::string dispatcher = "http://192.168.30.112:6666";

std::chrono::time_point<std::chrono::high_resolution_clock> shutdownPrimaryNode() {
  CURL *curl;
  CURLcode res;

  std::chrono::time_point<std::chrono::high_resolution_clock> performRequestTime;
 
  curl_global_init(CURL_GLOBAL_ALL);

  curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, (primaryNode + "/shutdown/").c_str());

    res = curl_easy_perform(curl);

    performRequestTime = std::chrono::high_resolution_clock::now();

    if(res != CURLE_OK)
      fprintf(stderr, "!!!! curl_easy_perform() failed: %s\n",
              curl_easy_strerror(res));
    else
      std::cout << "Shut down send to primary instance" << std::endl;
 
    curl_easy_cleanup(curl);
  }
  curl_global_cleanup();

  return performRequestTime;
}

struct string {
  char *ptr;
  size_t len;
};

void init_string(struct string *s) {
  s->len = 0;
  s->ptr = (char*)malloc(250);
  s->ptr[0] = '\0';
}

size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s)
{
  memcpy(s->ptr, ptr, size*nmemb);
  s->ptr[size*nmemb] = '\0';
  s->len = size*nmemb;

  return size*nmemb;
}

int main(void)
{
  CURL *curl;
  CURLcode res;

  curl_global_init(CURL_GLOBAL_ALL);
 
  std::string noOpQuery = "query={\"operators\": { \
        \"NoOp\": { \
            \"type\" : \"NoOp\" \
        } \
    }, \
    \"edges\" : [ \
        [\"NoOp\", \"NoOp\"] \
    ] \
  }";

  curl = curl_easy_init();
  if (curl) {
    struct string s;
    init_string(&s);

    curl_easy_setopt(curl, CURLOPT_URL, (dispatcher + "/procedureRevenueInsert/").c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, noOpQuery.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
 
    for (int i = 0; i < 100; ++i) {
      res = curl_easy_perform(curl);

      if(res != CURLE_OK)
        fprintf(stderr, "NoOp(1) failed: %s\n",
                curl_easy_strerror(res));
      else {
        // printf("NoOp(1) Success\n");
        // std::cout << s.ptr[0] << std::endl;
      }
    }

    // Resetting reply buffer
    s.ptr[0] = 'N';

    std::cout << "Executed a hundred test requests" << std::endl;
 
    std::chrono::time_point<std::chrono::high_resolution_clock> killedMasterTime, messageFromNewMasterTime;

    killedMasterTime = shutdownPrimaryNode();

    curl_easy_setopt(curl, CURLOPT_URL, (dispatcher + "/procedureRevenueInsert/").c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, noOpQuery.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);

    while (s.ptr[0] != '{') {
      res = curl_easy_perform(curl);

      messageFromNewMasterTime = std::chrono::high_resolution_clock::now();

      if(res != CURLE_OK)
        fprintf(stderr, "NoOp(2) failed: %s\n",
                curl_easy_strerror(res));
      else {
        // printf("NoOp(2) Success\n");
      }
    }

    std::cout << "Received answer from new primary" << std::endl;

    auto difference = messageFromNewMasterTime - killedMasterTime;
    std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(difference);

    std::cout << "It took: " << ns.count() << " nano seconds" << std::endl;

    curl_easy_cleanup(curl);
  }
  curl_global_cleanup();
  return 0;
}
