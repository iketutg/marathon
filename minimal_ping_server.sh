#!/bin/sh
ncat -klv 8080 -c 'echo "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHi\r\n"'
