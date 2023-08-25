
## Build
```
docker build testing_containers/ldap_sudo_environment -t agent_posix_ldap_multiuser
```

## Run Interactive Bash
To start an interactive bash session:
```
docker run -h ldap.environment.internal --rm -v $(pwd):/code:ro -e PIP_INDEX_URL=${PIP_INDEX_URL} -it --entrypoint bash agent_posix_ldap_multiuser:latest
```
To start the LDAP Server and Client:
```
service slapd start && service nscd restart && service nslcd restart
```
Login via ldap:
```
login -p hostuser
```