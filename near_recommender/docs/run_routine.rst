
# model update takes 20min
# user posts > call pkg main() -> 10 k finest recomm -> updated in background -> time 5min -> if user is recommendable
# user posts > call pkg main() -> 10 k finest recomm -> updated in background
# user posts > call pkg main() -> 10 k finest recomm -> updated in background
# user posts > call pkg main() -> 10 k finest recomm -> updated in background
# model should be upated
# user posts > call pkg main() -> 10 k finest recomm -> updated in background
# user posts > call pkg main() -> 10 k finest recomm -> updated in background

# synchronizing the access to the pickled model, run_update_model
