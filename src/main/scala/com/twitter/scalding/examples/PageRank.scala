package com.twitter.scalding.examples

import scala.annotation.tailrec
import com.twitter.scalding._

/**
* Options:
* --input: the three column TSV with node, comma-sep-out-neighbors, initial pagerank (set to 1.0 first)
* --ouput: the name for the TSV you want to write to, same as above.
* optional arguments:
* --errorOut: name of where to write the L1 error between the input page-rank and the output
*   if this is omitted, we don't compute the error
* --iterations: how many iterations to run inside this job.  Default is 1, 10 is about as
*   much as cascading can handle.
* --jumpprob: probability of a random jump, default is 0.15
* --convergence: if this is set, after every "--iterations" steps, we check the error and see
*   if we should continue.  Since the error check is expensive (involving a join), you should
*   avoid doing this too frequently.  10 iterations is probably a good number to set.
* --temp: this is the name where we will store a temporary output so we can compare to the previous
*   for convergence checking.  If convergence is set, this MUST be.
*/
class PageRank(args : Args) extends Job(args) {

  //How many steps
  val STEPS = args.getOrElse("iterations","1").toInt
  //Probability of taking a random jump in the network.
  val ALPHA = args.getOrElse("jumpprob","0.15").toDouble
  //How many times have we checked for convergence:
  val JOB_COUNT = args.getOrElse("jobCount","0").toInt
  //These are constants used by the algorithm:
  val NODESET = 0
  val EDGE = 1

  //Read the input, this can be subclassed, but should produce a pipe with three
  //columns: source node, comma separated (no spaces) destination nodes as a string, and
  //initial rank (default to 1.0 if you are starting from nothing)
  initialize('src, 'dst, 'rank)
  /*
  * This algorithm works by having two types of rows that have the same column structure.
  * the node -> list(neighbors), and node -> individual neighbor.
  * We distinguish these two types with an id which nodes if this is a NODESET or an EDGE.
  * The first step is to append that value.  We also need to have a column for the degree.
  * It doesn't matter what the initial degree is, we recompute below
  */
    .map(() -> ('rowtype, 'd_src)) { (u:Unit) => (NODESET,-1) }
    .then( doPageRank(STEPS)_ )
    .then( computeError _ )
    .then( output _ )

  /**
  * Here is where we check for convergence and then run the next job if we're not converged
  */
  override def next : Option[Job] = {
    args.optional("convergence")
      .flatMap { convErr =>
        /*
         * It's easy for this to seem broken, so think about it twice:
         * We are swapping between two writing files: temp and output, with the ultimate
         * goal to land up at output.  So, each next input is this output, but the temp
         * and output should be swapping.
         */
        val nextArgs = args + ("input", Some(args("output"))) +
                              ("temp", Some(args("output"))) +
                              ("output", Some(args("temp"))) +
                              ("jobCount", Some((JOB_COUNT + 1).toString))
        //Actually read the error:
        val error = Tsv(args("errorOut")).readAtSubmitter[Double].head;
        // The last job should be even numbered so output is not in temp.
        // TODO: if we had a way to do HDFS operations easily (like rm, mv, tempname)
        // this code would be cleaner and more efficient.  As is, we may go a whole extra
        // set of operations past the point of convergence.
        if (error > convErr.toDouble || (JOB_COUNT % 2 == 1)) {
          //try again to get under the error
          Some(clone(nextArgs))
        }
        else {
          None
        }
      }
  }
  /**
  * override this function to change how you generate a pipe of
  * (Long, String, Double)
  * where the first entry is the nodeid, the second is the list of neighbors,
  * as a comma (no spaces) separated string representation of the numeric nodeids,
  * the third is the initial page rank (if not starting from a previous run, this
  * should be 1.0
  *
  * NOTE: if you want to run until convergence, the initialize method must read the same
  * EXACT format as the output method writes.  This is your job!
  */
  def initialize(nodeCol : Symbol, neighCol : Symbol, pageRank : Symbol) = {
    Tsv(args("input")).read
      //Just to name the columns:
      .mapTo((0,1,2)->(nodeCol, neighCol, pageRank)) {
        input : (Long, String, Double) => input
      }
  }

  /**
  * The basic idea is to groupBy the dst key with BOTH the nodeset and the edge rows.
  * the nodeset rows have the old page-rank, the edge rows are reversed, so we can get
  * the incoming page-rank from the nodes that point to each destination.
  */

  @tailrec
  final def doPageRank(steps : Int)(pagerank : RichPipe) : RichPipe = {
    if( steps <= 0 ) { pagerank }
    else {
      val nodeRows = pagerank
        //remove any EDGE rows from the previous loop
        .filter('rowtype) {  (rowtype : Int) => rowtype == NODESET }
        //compute the incremental rank due to the random jump:
      val randomJump = nodeRows.map('rank -> 'rank) { (rank : Double) => ALPHA }
      //expand the neighbor list inte an edge list and out-degree of the src
      val edges = nodeRows.flatMap(('dst,'d_src) -> ('dst,'d_src)) { args : (String, Long) =>
          if (args._1.length > 0) {
            val dsts = args._1.split(",")
            //Ignore the old degree:
            val deg = dsts.size
            dsts.map { str => (str.toLong, deg) }
          }
          else {
            //Here is a node that points to no other nodes (dangling)
            Nil
          }
        }
        //Here we make a false row that we use to tell dst how much incoming
        //Page rank it needs to add to itself:
        .map(('src,'d_src,'dst,'rank,'rowtype)->('src,'d_src,'dst,'rank,'rowtype)) {
          intup : (Long,Long,Long,Double,Int) =>
            val (src : Long, d_src : Long, dst : Long, rank : Double, row : Int) = intup
            //The d_src, and dst are ignored in the merge below
            //We swap destination into the source position
            (dst, -1L, "", rank*(1.0 - ALPHA)/ d_src, EDGE)
         }
      /**
      * Here we do the meat of the algorithm:
      * if N = number of nodes, pr(N_i) prob of walking to node i, then:
      * N pr(N_i) = (\sum_{j points to i} N pr(N_j) * (1-ALPHA)/d_j) + ALPHA
      * N pr(N_i) is the page rank of node i.
      */
      val nextPr = (edges ++ randomJump).groupBy('src) {
        /*
         * Note that NODESET < EDGE, so if we take the min(rowtype, ...)
         * using dictionary ordering, we only keep NODESET rows UNLESS
         * there are rows that had no outdegrees, so they had no NODESET row
         * to begin with.  To fix the later case, we have to additionally
         * filter the result to keep only NODESET rows.
         */
        _.min('rowtype, 'dst, 'd_src)
         .sum('rank) //Sum the page-rank from both the nodeset and edge rows
      }
      //Must call ourselves in the tail position:
      doPageRank(steps-1)(nextPr)
    }
  }

  //This outputs in the same format as the input, so you can run the job
  //iteratively, subclass to change the final behavior
  def output(pipe : RichPipe) = {
    pipe.project('src, 'dst, 'rank).write(Tsv(args("output")))
  }

  //Optionally compute the average error:
  def computeError(pr : RichPipe) : RichPipe = {
    args.optional("errorOut").map { errOut =>
      Tsv(args("input")).read
        .mapTo((0,1,2)->('src0, 'dst0, 'rank0)) { tup : (Long, String, Double) => tup }
        .joinWithSmaller('src0 -> 'src, pr)
        .mapTo(('rank0,'rank) -> 'err) { ranks : (Double, Double) =>
          scala.math.abs(ranks._1 - ranks._2)
        }
        .groupAll { _.average('err) }
        .write(Tsv(errOut))
    }
    pr
  }
}
