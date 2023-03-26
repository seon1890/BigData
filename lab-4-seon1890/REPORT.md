# Lab 4: Dask

- Name: Seonhye Yang

- NetID: sy3420

## Description of your solution
1. What strategies did you use to optimize the computation for the full set?
    Increased partition size by the file size. By increasing the number of partitions, we can split the large dataframes so it can be processed         since we are using 1 core and 1 node. Lastly, I changed LOCAL to FALSE. 
2. Did you try any alternative implementations that didn't work, or didn't work as well? If so, how did this change your approach?
    Having no parition did not work on the all files. Also, it was tricky determining the number of partitions. 
    In order to determine the number of the partitions for large files, I checked the number of rows for the tiny files, which was n. Since my         code could run the tiny file on partition = 1, I determined the number of the partitions for the all files by taking the total number of rows       for all files and dividing it by n. In this case, it is int(len(files)/n. 
    

3. Did you encounter any unexpected behavior?
    One of the unexpected behaviors I ran into was the jupyter notebook crashing. If it could not run, then I would run a new session. This was         because the files were too large to run. So I had to do npartition in order to split the dataframe so 1 core and 1 node would process the           dataframe. Efficient code is key in this case.
    
    Also, my computer has 8 cores and I thought the large files would run on my computer much better than the hpc 1 core and 1 node. However, this     notebook was made in a way so it cannot run on a local machine such as my computer. There are functions that I do not access to if I run on my     computer. 
