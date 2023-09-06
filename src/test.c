#include <stdio.h>
#include <string.h>
// info locals 
// l 
// b 4
// n
// c 
// info args

int main() {
    unsigned long long int n, sum;
    n = 1;
    sum = 0;

    size_t t = strlen("\r\n");
    printf("%d %u\n", strlen("\r\n"), strlen("\r\n"));

    while (n <= 100) {
        sum += n;
        n++;
    }

    return 0;
}