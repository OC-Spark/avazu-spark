package org.ocspark.avazu.base.mark.mark1

import scopt.OptionParser

object Train {

//#include <iostream>
//#include <algorithm>
//#include <stdexcept>
//#include <omp.h>

//#include "common.h"
//#include "timer.h"

//namespace {

/*class Option{
//    Option() 
//        : eta(0.1f), lambda(0.00002f), iter(15), nr_factor(4), nr_factor_real(4), 
//          nr_threads(1), do_prediction(true) {}
    var Tr_path : String
    var Va_path : String
    var eta : Float = 0.1f
    var lambda : Float
    var iter : Int
    var nr_factor : Int
    var nr_factor_real : Int
    var nr_threads : Int
    var do_prediction : Boolean
};*/

  def train_help() : String = {
      val helpString =
  "usage: fm [<options>] <validation_path> <train_path>\n"
  "\n"
  "<validation_path>.out will be automatically generated at the end of training\n"
  "\n"
  "options:\n"
  "-l <lambda>: set the regularization penalty\n"
  "-k <factor>: set the number of latent factors, which must be a multiple of 4\n"
  "-t <iteration>: set the number of iterations\n"
  "-r <eta>: set the learning rate\n"
  "-s <nr_threads>: set the number of threads\n"
  "-q: if it is set, then there is no output file\n"
        helpString
  }

  case class Params(
      iter: Int = 15,
      eta: Float = 0.1f,
      lambda: Float = 0.00002f,
      nr_factor: Int = 4,
      nr_factor_real : Int = 4,
      nr_threads : Int = 1,
      do_prediction : Boolean = true,
      va_path : String = null,
      tr_path : String = null) 

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("SparseNaiveBayes") {
      head("SparseNaiveBayes: an example naive Bayes app for LIBSVM data.")
      opt[Int]("t")
        .text("number of iterations")
        .action((x, c) => c.copy(iter = x))
      opt[Int]("k")
        .text("number of factors")
        .action((x, c) => {
          c.copy(nr_factor_real = x)
          c.copy(nr_factor = (Math.ceil(x.toFloat/4.0f)*4).toInt)
        })
      opt[Double]("r")
        .text(s"eta default:${defaultParams.eta}")
        .action((x, c) => c.copy(eta = x.toFloat))
      opt[Double]("l")
        .text(s"lambda default:${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x.toFloat))
      opt[Int]("s")
        .text("number of threads")
        .action((x, c) => c.copy(nr_threads = x))
      opt[Boolean]("q")
        .text("Do prediction")
        .action((x, c) => c.copy(do_prediction = x))
      arg[String]("<validation_path>")
        .text("validation path")
        .required()
        .action((x, c) => c.copy(va_path = x))
      arg[String]("<train_path>")
        .text("training path")
        .required()
        .action((x, c) => c.copy(tr_path = x))
    }

    parser.parse(args, defaultParams).map { params =>
//      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }
}

/*def parse_option(args : Array[String]) : Option = {
    val argc = args.length

    if(argc == 0){
        println(train_help())
        return null
    }

    var opt : Option = null

    for (var i <- 0 to (argc - 1))
    {
        if(args(i) == ("-t")){
            if (i == argc-1){
                println("invalid command\n");
                return null
            }
            val j = i+1
            opt.iter = args(j).toInt
        }
        else if(args[i].compare("-k") == 0)
        {
            if(i == argc-1)
                throw std::invalid_argument("invalid command\n");
            opt.nr_factor_real = std::stoi(args[++i]);
            opt.nr_factor = static_cast<uint32_t>(ceil(static_cast<float>(opt.nr_factor_real)/4.0f)*4);
        }
        else if(args[i].compare("-r") == 0)
        {
            if(i == argc-1)
                throw std::invalid_argument("invalid command\n");
            opt.eta = std::stof(args[++i]);
        }
        else if(args[i].compare("-l") == 0)
        {
            if(i == argc-1)
                throw std::invalid_argument("invalid command\n");
            opt.lambda = std::stof(args[++i]);
        }
        else if(args[i].compare("-s") == 0)
        {
            if(i == argc-1)
                throw std::invalid_argument("invalid command\n");
            opt.nr_threads = std::stoi(args[++i]);
        }
        else if(args[i].compare("-q") == 0)
        {
            opt.do_prediction = false;
        }
        else
        {
            break;
        }

    }

    if(i >= argc-1)
        throw std::invalid_argument("training or test set not specified\n");

    opt.Va_path = args[i++];
    opt.Tr_path = args[i++];

    return opt;
}
  
  def run(params: Params) {
    
  }

def init_model(model : Model, nr_factor_real : Int){
    val nr_factor = model.nr_factor;
    val coef : Float = (0.5/Math.sqrt(nr_factor)).toFloat;

    val w = model.W.data()
    for(uint32_t j = 0; j < model.nr_feature; ++j)
    {
        for(uint32_t f = 0; f < model.nr_field; ++f)
        {
            for(uint32_t d = 0; d < nr_factor_real; ++d, ++w)
                *w = coef*static_cast<float>(drand48());
            for(uint32_t d = nr_factor_real; d < nr_factor; ++d, ++w)
                *w = 0;
            for(uint32_t d = nr_factor; d < 2*nr_factor; ++d, ++w)
                *w = 1;
        }
    }
}

void train(Problem const &Tr, Problem const &Va, Model &model, Option const &opt)
{
    std::vector<uint32_t> order(Tr.Y.size());
    for(uint32_t i = 0; i < Tr.Y.size(); ++i)
        order[i] = i;

    Timer timer;
    printf("iter     time    tr_loss    va_loss\n");
    for(uint32_t iter = 0; iter < opt.iter; ++iter)
    {
        timer.tic();

        double Tr_loss = 0;
        //std::random_shuffle(order.begin(), order.end());
#pragma omp parallel for schedule(static)
        for(uint32_t i_ = 0; i_ < order.size(); ++i_)
        {
            uint32_t const i = order[i_];

            float const y = Tr.Y[i];
            
            float const t = wTx(Tr, model, i);

            float const expnyt = static_cast<float>(exp(-y*t));

            Tr_loss += log(1+expnyt);
               
            float const kappa = -y*expnyt/(1+expnyt);

            wTx(Tr, model, i, kappa, opt.eta, opt.lambda, true);
        }
        Tr_loss /= static_cast<double>(Tr.Y.size());

        double const Va_loss = predict(Va, model);

        printf("%4d %8.1f %10.5f %10.5f\n", 
               iter, timer.toc(), Tr_loss, Va_loss);
        fflush(stdout);
    }
}

} //unnamed namespace

int main(int const argc, char const * const * const argv)
{
    Option opt;
    try
    {
        opt = parse_option(argv_to_args(argc, argv));
    }
    catch(std::invalid_argument const &e)
    {
        std::cout << e.what();
        return EXIT_FAILURE;
    }

    std::cout << "reading data..." << std::flush;
    Problem const Va = read_problem(opt.Va_path);
    Problem const Tr = read_problem(opt.Tr_path);
    std::cout << "done\n" << std::flush;

    std::cout << "initializing model..." << std::flush;
    Model model(Tr.nr_feature, opt.nr_factor, Tr.nr_field);
    init_model(model, opt.nr_factor_real);
    std::cout << "done\n" << std::flush;

	omp_set_num_threads(static_cast<int>(opt.nr_threads));

    train(Tr, Va, model, opt);

	omp_set_num_threads(1);

    if(opt.do_prediction)
        predict(Va, model, opt.Va_path+".prd");

    return EXIT_SUCCESS;
}*/
