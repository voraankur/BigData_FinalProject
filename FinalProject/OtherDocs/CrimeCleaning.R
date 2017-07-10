mydata = read.csv("/home/ankur/workspace/FP2/input/Crimes_-_2001_to_present.csv")

datawithNA <- sub(pattern = "", NA, mydata)