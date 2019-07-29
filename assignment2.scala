val inputFilePath = "/tmp_amd/ravel/export/ravel/4/z5136463/Downloads/sample_input.txt"
val outputDirPath = "/tmp_amd/ravel/export/ravel/4/z5136463/Downloads/output"
//Give the path to value inputFilePath

val inputfile = sc.textFile(inputFilePath,1)
//Create RDD by using sc.textFile func in scala, and give the value to inputfile

val input1 = inputfile.map(x=>x.split(" ")(0)).filter(_.length>0)
//Delete empty row of the input text file.

val input2 = input1.map(x=>(x.split(",/")(0), x.split(",")(3)))
//Only keep the first object and third object of the rows, which are website address and size.


val input3 = input2.map(x=>(
	if(x._2.contains("MB")){
		(x._1, x._2.replace("MB","").toLong*1024*1024);
	}
	else if(x._2.contains("KB")){
		(x._1, x._2.replace("KB","").toLong*1024);
	}
	else if(x._2.contains("B")){
		(x._1, x._2.replace("B","").toLong);
	}
)
)
//change MB size and KB size to B size.

val input4 = input3.map(x=>(x.toString))
//As the type of array becomes array[any] in the above step, so I change the type into string.

val input5 = input4.map(x=>(x.replace("(",""))).map(x=>(x.replace(")","")))
//remove the "(" and ")" of the string

val input6 = input5.map(x=>(x.split(",")(0), x.split(",")(1)))
//split the string into website and size.

val input7 = input6.map(x=>(x._1, x._2.toLong))
//convert the size string to long type.

val input8 = input7.groupByKey()
//group the key value by key.

val input9 = input8.map(x=>(x._1, x._2.toArray))
//convert the iterable to array.

def average(payloads: Array[Long]): Long = {
    payloads.sum/payloads.length
}
//define a function to calculate the mean.

def getva( ts: Array[Long]): Long = {
	var varsum : Long = 0
	var med : Long = 0
	for(e <- ts){
		med = e - average(ts)
		varsum += med*med
	}
	varsum = varsum / ts.size.toLong
	varsum
}
//define a function to calculate the variance.

val input10 = input9.map(x=>(x._1, x._2.min, x._2.max, average(x._2), getva(x._2)))
//get the final result.

val input11 = input10.map(x=>(x._1, x._2.toString+"B", x._3.toString+"B", x._4.toString+"B", x._5.toString+"B"))
//change the long to string, and add "B" in the end.

val input12 = input11.map(x=>(x.toString))
//chang the whole (string,string,string,string,string) to one string. 

val input13 = input12.map(x=>(x.replace("(",""))).map(x=>(x.replace(")","")))
//remove the "(" and ")" again.

input13.saveAsTextFile(outputDirPath)
//save strings in output.
















