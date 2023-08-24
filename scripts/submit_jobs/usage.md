
# To submit a sleep job:

1. Create your service resources using `scripts/create_service_resources.sh`
2. `source .deployed_resources.sh`
3. `scripts/submit_jobs/sleep/submit_sleep.sh`

# To submit the asset example job

1. Create your service resources using `scripts/create_service_resources.sh`
2. `deadline config set defaults.aws_profile_name $AWS_DEFAULT_PROFILE`
3. `hatch shell`
4. `source .deployed_resources.sh`
5. `cd scripts/submit_jobs`
6. `deadline bundle submit --farm-id $FARM_ID --queue-id $QUEUE_ID --submit-as-json --yes -p DataDir=asset_example asset_example`
