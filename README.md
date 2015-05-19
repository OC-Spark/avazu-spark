# avazu-spark
spark implementation of avazu competition in kaggle

When converting all the preprocessing python scripts under package base, untar the attached avazu-base.tar in a test directory. Run the corresponding python script and compared the output to the original (already included in the tar). Thre are 5 python scripts involved in data preparation: gen_data.py, parallelizer.py, pickle_prediction.py, merge_prediction.py, unpickle_prediction.py

./util/gen_data.py ../tr.rx.csv ../va.rx.csv tr.rx.app.new.csv va.rx.app.new.csv tr.rx.site.new.csv va.rx.site.new.csv

util/parallelizer.py -s 12 converter/2.py tr.rx.app.new.csv va.rx.app.new.csv tr.rx.app.sp va.rx.app.sp

util/parallelizer.py -s 12 converter/2.py tr.rx.site.new.csv va.rx.site.new.csv tr.rx.site.sp va.rx.site.sp



./util/pickle_prediction.py va.rx.app.sp.prd va.rx.app.sp.prd.pickle
./util/pickle_prediction.py va.rx.site.sp.prd va.rx.site.sp.prd.pickle

./util/merge_prediction.py va.rx.app.sp.prd.pickle va.rx.site.sp.prd.pickle va.rx.prd.pickle 
./util/merge_prediction.py va.rx.app.sp.prd.pickle va.rx.site.sp.prd.pickle va.rx.prd.pickle 


./util/unpickle_prediction.py va.rx.prd.pickle base.rx.prd


For example, in the gen_data.py case, tr.rx.csv and va.rx.csv are input files. The script should generate four output files: tr.rx.app.new.csv va.rx.app.new.csv tr.rx.site.new.csv va.rx.site.new.csv. Once you convert gen_data.py to GenData.scala, run it using the same input files, and verify that you obtain identical output files. 

These way, we can be sure to convert all files correctly. 

----

instructions for setting up kaggle-avazu on you Mac

1. brew install gcc49

2. modify all Makefiles. change two lines in each.

CXX = g++-4.9
CXXFLAGS = -Wall -Wconversion -Wa,-q -O3 -fPIC -std=c++11 -march=native -fopenmp

3. python3 is required to run the python code. python3.4 comes with pip and is the easiest way to set up python dependencies properly.

