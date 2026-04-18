Always activate `.venv` before running Python.

Be very careful with fallback behaviour. A service should have a clear specific job, and should not fallback to bad defaults, do something unpected, or fall back to rarely used code paths. Fallbacks, where applicable, should be clear, explicit, and not do something unexpected. We should not use them to mask failures, errors etc. 

Loud logging. If something breaks, make sure to loudly emit an error or failure. If a non-happy-path is hits an unexpected output, or does something it shouldn't, eer on the side of emitting an error, rather than failing silently. 

Pay close attention to the non happy path. Use your intelligence to ensure that non-happy-path bugs do not occur. The happy path will not always be hit - it's your job to ensure that when the happy path is not hit, that the program behaves well. 

Use only the .parquet files in the data/ directory. Don't use any csv files. 