ARG SCYLLA_VERSION=2.3.1

FROM scylladb/scylla:$SCYLLA_VERSION

RUN sed -i -e "s/enable_user_defined_functions: false/enable_user_defined_functions: true/" /etc/scylla/scylla.yaml
