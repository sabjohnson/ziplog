Mar. 18, 2026

Current Tasks/Problems to Look Into:
- server liveness_tracker is not started on object creation. proxies still falsely reported. the data struct (deque) may need to be changed to something more robust.
- update single_proxy/twoepochs to check the commands while parsing out the timestamp
- Add condition variable for when proxy paused

Most Recent Additions:
- added logging preprocessor directive
- epoch duration unit. change alias definition in api/types.h to alter timing unit throughout the components.