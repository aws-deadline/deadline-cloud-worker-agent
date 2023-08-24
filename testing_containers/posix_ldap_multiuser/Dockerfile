# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

FROM public.ecr.aws/docker/library/python:3.9-buster

ENV AGENT_USER=agentuser
ENV JOB_USER=jobuser
ENV SHARED_GROUP=sharedgroup

# Use a docker volume to mount the root of the repo to this directory
VOLUME /code /aws

WORKDIR /config

COPY changePassword.ldif /config
COPY addUsersGroups.ldif /config
COPY addUsersToSharedGroup.ldif /config
COPY start_ldap.sh /config
COPY setup.sh /config
COPY run.sh /config
COPY run_agent.sh /config
COPY term_agent.sh /config

# We set up two users:
#  1) agentuser -- the user that will be running the Worker Agent.
#  2) jobuser -- the user that we'll be running Jobs as.

# These accounts belong to the following groups:
#   agentuser: agentuser, sharedgroup
#   jobuser: jobuser, sharedgroup
RUN chmod 777 /code /aws && \
    echo $(grep $(hostname) /etc/hosts | cut -f1) ldap.environment.internal >> /etc/hosts && \
    apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y vim screen slapd ldap-utils && \
    echo slapd   slapd/password1 password | debconf-set-selections -v && \
    echo slapd   slapd/password2 password | debconf-set-selections -v && \
    echo slapd   slapd/internal/adminpw  password | debconf-set-selections -v && \
    echo slapd   slapd/internal/generated_adminpw password | debconf-set-selections -v && \
    echo slapd   slapd/password_mismatch note | debconf-set-selections -v && \
    echo slapd   slapd/no_configuration  boolean false | debconf-set-selections -v && \
    echo slapd   slapd/dump_database     select  when needed | debconf-set-selections -v && \
    echo slapd   slapd/domain    string  environment.internal | debconf-set-selections -v && \
    echo slapd   slapd/move_old_database boolean true | debconf-set-selections -v && \
    echo slapd   slapd/postinst_error    note | debconf-set-selections -v && \
    echo slapd   slapd/purge_database    boolean false | debconf-set-selections -v && \
    echo slapd   slapd/dump_database_destdir     string  /var/backups/slapd-VERSION | debconf-set-selections -v && \
    echo slapd   shared/organization     string  environment.internal | debconf-set-selections -v && \
    echo slapd   slapd/invalid_config    boolean true | debconf-set-selections -v && \
    echo slapd   slapd/upgrade_slapcat_failure   error | debconf-set-selections -v && \
    dpkg-reconfigure slapd && \
    echo "BASE    dc=environment,dc=internal" >> /etc/ldap/ldap.conf && \
    echo "URI     ldap://ldap.environment.internal" >> /etc/ldap/ldap.conf && \
    update-rc.d slapd enable && service slapd start && \
    ldapmodify -Q -Y EXTERNAL -H ldapi:/// -f /config/changePassword.ldif && \
    ldapadd -x -D cn=admin,dc=environment,dc=internal -w password -f /config/addUsersGroups.ldif && \
    ldapmodify -xcD cn=admin,dc=environment,dc=internal -w password -f /config/addUsersToSharedGroup.ldif && \
    echo nslcd   nslcd/ldap-uris string  ldap://ldap.environment.internal/ | debconf-set-selections -v && \
    echo nslcd   nslcd/ldap-base string  dc=environment,dc=internal | debconf-set-selections -v && \
    echo libnss-ldapd:amd64      libnss-ldapd/nsswitch   multiselect     passwd, group, shadow | debconf-set-selections -v && \
    apt-get install -y libnss-ldapd libpam-ldapd && \
    echo session optional        pam_mkhomedir.so skel=/etc/skel umask=007 >> /etc/pam.d/common-session && \
    touch /etc/netgroup && service nscd restart && service nslcd restart && \
    # Create the home directories before login
    mkhomedir_helper ${AGENT_USER} 002 && mkhomedir_helper ${JOB_USER} 002 && \
    # Set up the sudoers permissions
    apt-get install sudo-ldap && \
    echo "${AGENT_USER} ALL=(${JOB_USER},${AGENT_USER}) NOPASSWD: ALL" > /etc/sudoers.d/${AGENT_USER}


WORKDIR /home/agentuser

CMD ["/bin/sh", "-c", "/config/start_ldap.sh && /config/setup.sh && /config/run.sh"]