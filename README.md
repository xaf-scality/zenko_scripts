Set of one-off scripts for dealing with Zenko

bucket_destruction.py:  removes all versions, delete markers and unfinished 
                        MPUS from a bucket. There's a script in S3Utils that 
                        handles this now but I like minte better since it's no
                        nodejs requiring a bunch of 'npm installs' :-)

crr_failure_trigger.py: This will send a mail to a configured address when 
                        there are multiple permanenet CRR failures to a 
                        location.

get_group_lag.py:       Just displays the lag in partitions for location
                        topics

list_crr_backlog.py:    Displays objects in back-log for a given location. It
                        is better to use MD search to do this but this uses
                        kafka directly

reset_redis.py:         Resets pending and failed counters to clean-up Orbit 
                        UI in case it gets out of sync during testing.