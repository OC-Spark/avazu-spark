package org.ocspark.avazu.base.mark.mark1

import java.util.Properties
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ArrayBuffer
import scala.inline

object Common {
  
/* #pragma GCC diagnostic ignored "-Wunused-result"

#ifndef _COMMON_H_
#define _COMMON_H_

#define flag { printf("\nLINE: %d\n", __LINE__); fflush(stdout); }

#include <vector>
#include <cmath>
#include <pmmintrin.h>*/

//  class Problem {
//    def Problem(nr_instance : Int, nr_field : Int) {
//        this.nr_instance = nr_instance
//        this.nr_field = nr_field
//        this.v = (2.0f/nr_field.toFloat) 
//        this.J = ArrayBuffer[Int](nr_instance * nr_field)
//        this.Y = ArrayBuffer[Float](nr_instance)
//        this.Z = ArrayBuffer[String](nr_instance + "")
//    }
//    var nr_feature : Int = 0
//    var nr_instance : Int = 1
//    var nr_field : Int
//    var v : Float
//    var J : ArrayBuffer[Int]
//    var Y : ArrayBuffer[Float]
//    var Z : ArrayBuffer[String]
//  }

//Problem read_problem(std::string const path);

  val kW_NODE_SIZE : Int = 2;
  
//  class Model {
//      def Model(nr_feature : Int, nr_factor : Int, nr_field : Int) {
//          this.W = ArrayBuffer[Float](nr_feature * nr_field * nr_factor * kW_NODE_SIZE, 0)
//          this.nr_feature = nr_feature
//          this.nr_factor = nr_factor
//          this.nr_field = nr_field
//      }
//      var W : ArrayBuffer[Float]
//      var nr_feature : Int
//      var nr_factor : Int
//      var nr_field : Int
//  };

//FILE *open_c_file(std::string const &path, std::string const &mode);

//std::vector<std::string> 
//argv_to_args(int const argc, char const * const * const argv);

/*#if defined NOSSE

inline float qrsqrt(float x)
{
    _mm_store_ss(&x, _mm_rsqrt_ps(_mm_load1_ps(&x)));
    return x;
}

inline float wTx(Problem const &prob, Model &model, uint32_t const i, 
    float const kappa=0, float const eta=0, float const lambda=0, 
    bool const do_update=false)
{
    uint32_t const nr_factor = model.nr_factor;
    uint32_t const nr_field = model.nr_field;
    uint32_t const nr_feature = model.nr_feature;
    uint64_t const align0 = nr_factor*kW_NODE_SIZE;
    uint64_t const align1 = nr_field*align0;

    uint32_t const * const J = &prob.J[i*nr_field];
    float * const W = model.W.data();

    float const v = prob.v;
    float const kappav = kappa*v;

    float t = 0;
    for(uint32_t f1 = 0; f1 < nr_field; ++f1)
    {
        uint32_t const j1 = J[f1];
        if(j1 >= nr_feature)
            continue;

        for(uint32_t f2 = f1+1; f2 < nr_field; ++f2)
        {
            uint32_t const j2 = J[f2];
            if(j2 >= nr_feature)
                continue;

            float * w1 = W + j1*align1 + f2*align0;
            float * w2 = W + j2*align1 + f1*align0;

            if(do_update)
            {
                float * wg1 = w1 + nr_factor;
                float * wg2 = w2 + nr_factor;
                for(uint32_t d = 0; d < nr_factor; ++d)
                {
                    float const g1 = lambda*w1[d] + kappav*w2[d];
                    float const g2 = lambda*w2[d] + kappav*w1[d];

                    wg1[d] += g1*g1;
                    wg2[d] += g2*g2;

                    w1[d] -= eta*qrsqrt(wg1[d])*g1;
                    w2[d] -= eta*qrsqrt(wg2[d])*g2;

                }
            }
            else
            {
                for(uint32_t d = 0; d < nr_factor; ++d)
                    t += w1[d]*w2[d]*v;
            }
        }
    }

    return t;
}

#else

def wTx(prob : Problem, model : Model, i : Int, 
    kappa : Float = 0, eta : Float = 0, lambda : Float = 0, 
    do_update : Boolean = false) : Float = {
    val nr_factor : Int = model.nr_factor
    val nr_field : Int = model.nr_field
    val nr_feature : Int = model.nr_feature
    val align0 : Int = nr_factor * kW_NODE_SIZE
    val align1 : Int = nr_field * align0

    uint32_t const * const J = &prob.J[i*nr_field];
    val W : Float = model.W.data()

    __m128 const XMMv = _mm_set1_ps(prob.v);
    __m128 const XMMkappav = _mm_set1_ps(kappa*prob.v);
    __m128 const XMMeta = _mm_set1_ps(eta);
    __m128 const XMMlambda = _mm_set1_ps(lambda);

    __m128 XMMt = _mm_setzero_ps();
    for(uint32_t f1 = 0; f1 < nr_field; ++f1)
    {
        uint32_t const j1 = J[f1];
        if(j1 >= nr_feature)
            continue;

        for(uint32_t f2 = f1+1; f2 < nr_field; ++f2)
        {
            uint32_t const j2 = J[f2];
            if(j2 >= nr_feature)
                continue;

            float * const w1 = W + j1*align1 + f2*align0;
            float * const w2 = W + j2*align1 + f1*align0;

            if(do_update)
            {
                float * const wg1 = w1 + nr_factor;
                float * const wg2 = w2 + nr_factor;
                for(uint32_t d = 0; d < nr_factor; d += 4)
                {
                    __m128 XMMw1 = _mm_load_ps(w1+d);
                    __m128 XMMw2 = _mm_load_ps(w2+d);

                    __m128 XMMwg1 = _mm_load_ps(wg1+d);
                    __m128 XMMwg2 = _mm_load_ps(wg2+d);

                    __m128 XMMg1 = _mm_add_ps(
                                   _mm_mul_ps(XMMlambda, XMMw1),
                                   _mm_mul_ps(XMMkappav, XMMw2));
                    __m128 XMMg2 = _mm_add_ps(
                                   _mm_mul_ps(XMMlambda, XMMw2),
                                   _mm_mul_ps(XMMkappav, XMMw1));

                    XMMwg1 = _mm_add_ps(XMMwg1, _mm_mul_ps(XMMg1, XMMg1));
                    XMMwg2 = _mm_add_ps(XMMwg2, _mm_mul_ps(XMMg2, XMMg2));

                    XMMw1 = _mm_sub_ps(XMMw1, _mm_mul_ps(XMMeta, 
                            _mm_mul_ps(_mm_rsqrt_ps(XMMwg1), XMMg1)));
                    XMMw2 = _mm_sub_ps(XMMw2, _mm_mul_ps(XMMeta, 
                            _mm_mul_ps(_mm_rsqrt_ps(XMMwg2), XMMg2)));

                    _mm_store_ps(w1+d, XMMw1);
                    _mm_store_ps(w2+d, XMMw2);

                    _mm_store_ps(wg1+d, XMMwg1);
                    _mm_store_ps(wg2+d, XMMwg2);
                }
            }
            else
            {
                for(uint32_t d = 0; d < nr_factor; d += 4)
                {
                    __m128 const XMMw1 = _mm_load_ps(w1+d);
                    __m128 const XMMw2 = _mm_load_ps(w2+d);

                    XMMt = _mm_add_ps(XMMt, 
                           _mm_mul_ps(_mm_mul_ps(XMMw1, XMMw2), XMMv));
                }
            }
        }
    }

    if(do_update)
        return 0;

    XMMt = _mm_hadd_ps(XMMt, XMMt);
    XMMt = _mm_hadd_ps(XMMt, XMMt);
    float t;
    _mm_store_ss(&t, XMMt);

    return t;
}

#endif

float predict(Problem const &prob, Model &model, 
    std::string const &output_path = std::string(""));
#endif // _COMMON_H_*/


	val kMaxLineSize = 1000000;
	
	   val (hdfsHost) =
    try {
      val prop = new Properties()
      val is = this.getClass().getClassLoader()
        .getResourceAsStream("config.properties");
      prop.load(is)

      (
        prop.getProperty("hdfs.host")
        )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }
    

  val conf = new Configuration()
//  val hdfsCoreSitePath = new Path("core-site.xml")
//  conf.addResource(hdfsCoreSitePath)
  val fs = FileSystem.get(conf)
  
	def get_nr_line(path : String) : Int = {
//	    val in = fs.open(path)
//	    val bufferArray = new Array[Byte](kMaxLineSize)
//	
//	    val nr_line = 0;
//	    
//	    while(fgets(line, kMaxLineSize, f) != nullptr)
//	        ++nr_line;
//	
//	    fclose(f);
//	
//	    nr_line;
    1
	}
	
	

def get_nr_field(path : String) : Int = {
//    FILE *f = open_c_file(path.c_str(), "r");
//    char line[kMaxLineSize];
//
//    fgets(line, kMaxLineSize, f);
//    strtok(line, " \t");
//    strtok(nullptr, " \t");
//
//    uint32_t nr_field = 0;
//    while(1)
//    {
//        char *idx_char = strtok(nullptr," \t");
//        if(idx_char == nullptr || *idx_char == '\n')
//            break;
//        ++nr_field;
//    }
//
//    fclose(f);
//
//    return nr_field;
  1
}

 //unamed namespace

/*def read_problem( path : String) : Problem = {
//    if(path.empty())
//        return Problem(0, 0);
//    Problem prob(get_nr_line(path), get_nr_field(path));
//
//    FILE *f = open_c_file(path.c_str(), "r");
//    char line[kMaxLineSize];
//
//    uint64_t p = 0;
//    for(uint32_t i = 0; fgets(line, kMaxLineSize, f) != nullptr; ++i)
//    {
//        char *z_char = strtok(line, " \t");
//        prob.Z[i] = std::string(z_char);
//
//        char *y_char = strtok(nullptr, " \t");
//        float const y = (atoi(y_char)>0)? 1.0f : -1.0f;
//        prob.Y[i] = y;
//        for(; ; ++p)
//        {
//            char *idx_char = strtok(nullptr," \t");
//            if(idx_char == nullptr || *idx_char == '\n')
//                break;
//            uint32_t idx = static_cast<uint32_t>(atoi(idx_char));
//            prob.nr_feature = std::max(prob.nr_feature, idx);
//            prob.J[p] = idx-1;
//        }
//    }
//
//    fclose(f);
//
//    return prob;
  null
}*/

/*FILE *open_c_file(std::string const &path, std::string const &mode)
{
    FILE *f = fopen(path.c_str(), mode.c_str());
    if(!f)
        throw std::runtime_error(std::string("cannot open ")+path);
    return f;
}

std::vector<std::string> 
argv_to_args(int const argc, char const * const * const argv)
{
    std::vector<std::string> args;
    for(int i = 1; i < argc; ++i)
        args.emplace_back(argv[i]);
    return args;
}

float predict(Problem const &prob, Model &model, 
    std::string const &output_path)
{
    FILE *f = nullptr;
    if(!output_path.empty())
    {
        f = open_c_file(output_path, "w");
        fprintf(f, "id,click\n");
    }

    double loss = 0;
    #pragma omp parallel for schedule(static) reduction(+:loss)
    for(uint32_t i = 0; i < prob.Y.size(); ++i)
    {
        float const y = prob.Y[i];

        float const t = wTx(prob, model, i);
        
        float const pred = 1/(1+static_cast<float>(exp(-t)));

        float const expnyt = static_cast<float>(exp(-y*t));

        loss += log(1+expnyt);

        if(f)
            fprintf(f, "%s,%.5lf\n", prob.Z[i].c_str(), pred);
    }

    if(f)
        fclose(f);

    return static_cast<float>(loss/static_cast<double>(prob.Y.size()));
}*/
}
//#include <stdexcept>
//#include <cstring>
//#include <omp.h>

//#include "common.h"



