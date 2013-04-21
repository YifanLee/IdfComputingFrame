IdfComputingFrame
=================

a map-reduce frame of idf (TF-IDF) computing for weibo data

=================

/* compute IDF values
 * 执行：hadoop xx.jar HDFSFileName.input HDFSFileName.output IDFterms_file
 * 输入：hdfs: IDs as the samples of IDF computing
 * 输出：hdfs:  the occurrence number of each IDF term, 
 * WARNING!!! the 0 occureence term will not be output.
 * IDFterms_file  the file containing all the corpus terms
 */
