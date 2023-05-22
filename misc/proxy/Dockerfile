FROM openresty/openresty:alpine

RUN	apk --update add --no-cache --virtual .gettext gettext

ADD proxy.conf /etc/nginx/conf.d/proxy.tmpl

ENV LIMIT                0
ENV DELAY                0.0
ENV UPSTREAM             http://www.example.com
ENV RESOLVER             8.8.8.8
ENV START_DELAY 0
ENV END_DELAY 0
ENV MY_PEERSET_START_DELAY 0
ENV MY_PEERSET_END_DELAY 0
ENV NOT_MY_PEERSET_START_DELAY 0
ENV NOT_MY_PEERSET_END_DELAY 0
ENV MY_PEERSET_DELAY 0
ENV NOT_MY_PEERSET_DELAY 0

CMD /bin/ash -c " \
        envsubst '\${LIMIT} \${UPSTREAM} \${DELAY} \${RESOLVER} \${PEERSET_ID} \${TYPE} \${START_DELAY} \${END_DELAY} \${MY_PEERSET_START_DELAY} \${MY_PEERSET_END_DELAY} \${NOT_MY_PEERSET_START_DELAY} \${NOT_MY_PEERSET_END_DELAY} \${MY_PEERSET_DELAY} \${NOT_MY_PEERSET_DELAY} \${FIXED_DELAY}' \
                 < /etc/nginx/conf.d/proxy.tmpl > /etc/nginx/conf.d/default.conf \
        && nginx -g 'daemon off;'"
