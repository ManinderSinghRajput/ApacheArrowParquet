Notes:

-> Data is shell generated and not genuine it's just for testing

-> Lots of assumptions are made through the code . Like data line would be in valid format like every field would be in respective data type. Error handling could be done to encounter such cases.

-> To ease things up and automate things Makefile is implemented. Recommended to make use of it.
   1. Use _make build_ to make fresh build of server as well as testing client.
   2. Use _make run_ to run the server alone , do manipulate the variable ARGS in Makefile to change input file
   3. Use _make prep_ to validate the code and check every thing is in right state.

-> UT not implemented yet

-> Api to return result for specified timeframe is not yet implemented