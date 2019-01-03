package com.amway.dms

import java.time.temporal.ChronoUnit.{MINUTES, SECONDS}
import java.time.temporal.IsoFields
import java.time.{LocalDateTime, MonthDay, Year, YearMonth}

import com.amway.dms.{Constant => C}

object Utils {

  type SymM = Map[Symbol, String]
  type SymL = List[Symbol]
  type PairSeq = Seq[(String, String)]

  def parse(args: Array[String]): (Map[Symbol, String], List[Symbol]) = {
    val map: SymM = Map()
    val list: SymL = List()

    def _parse(map: SymM, list: SymL, args: List[String]): (SymM, SymL) = {
      args match {
        case Nil => (map, list)
        case arg :: value :: tail if (arg.startsWith("--") && !value.startsWith("--")) =>
          _parse(map ++ Map(Symbol(arg.substring(2)) -> value), list, tail)
        //    case arg :: tail if (arg.startsWith("--")) => _parse(map ++ Map(Symbol(arg.substring(2)) -> true), list, tail)
        case opt :: tail => _parse(map, list :+ Symbol(opt), tail)
      }
    }

    val (opt_map, args_list) = _parse(map, list, args.toList)

    //    println(opt_map)
    //    println(args_list)

    (opt_map, args_list)
  }

  def verifyArgs(opt_map: SymM): (String, //input
    String, //output
    Option[String], // mode
    Option[String], // freq
    Seq[(String, String)], // range
    Boolean,
    Option[String] // prefix
    ) = {
    val usage =
      """
        | Usage: <program> --input <full_path_files
        | >
        |     --output <output_dir>
        |     --mode <last|upto|all>
        |     --freq <month|quarter>
        |     [--range <2017Q1, 2017M01-2018M03, 2018M04..2018M06> ]
        |     [--report-gen-prefix DMS ]
      """

    if (!opt_map.contains('input) || !opt_map.contains('output)) {
      println(C.ERR_INPUT_OUTPUT_MISSING);
      println(usage)
      sys.exit(1)
    }

    if (!opt_map.contains('range) && !opt_map.contains('mode) || (opt_map.contains('mode) &&
      !Seq(C.LAST, C.ALL, C.UPTO).contains(opt_map('mode).toString.toLowerCase))) {
      println(C.ERR_INVALID_MODE);
      println(usage)
      sys.exit(1)
    }

    if (!opt_map.contains('range) && !opt_map.contains('freq)  || (opt_map.contains('freq) &&
      !Seq(C.MM, C.QQ).contains(opt_map('freq).toString.toLowerCase))) {
      println(C.ERR_INVALID_FREQ);
      println(usage)
      sys.exit(1)
    }

    (opt_map('input).toString,
      opt_map('output).toString,
      opt_map.get('mode),
      opt_map.get('freq),
      rangexpr(opt_map.get('range)),
      opt_map.contains('range),
      opt_map.get(Symbol("report-gen-prefix")).orElse(Some(C.PREFIX))

    )
  }

  /*
     a:  the entire left part of the range
     b:  year of the left
     c:  delimiter
     d:  month of the left
     e:  sep char ( dash or double dots)
     f: the entire right part of the range
     g:  year of the right
     h:  month of the right
   */
  def rangexpr(str: Option[String]): PairSeq =
    if (str.isEmpty) Seq() else {
      str.get.toString split ",\\s*" flatMap { (s) =>
        val r = """((\d{4})([M|Q])(\d{1,2}))(?:(-|..)((\d{4})\3(\d{1,2})))?""".r
        s match {
          case r(a, b, c, d, e, f, g, h) if e == null =>
            if (validRange(c, d)) Seq((a, a)) else Seq()
          case r(a, b, c, d, e, f, g, h) =>
            if (validRange(c, d) && validRange(c, h)) expandRange(b, c, d, e, g, h) else Seq()
          case _ => Seq()
        }
      }
    }

  def expandRange(y1: String, delim: String, m1: String,
                  sep: String,
                  y2: String, m2: String): PairSeq = {
    val carry = delim match {
      case C.M => 12
      case C.Q => 4
    }

    val diff = (y2.toInt - y1.toInt) * carry + (m2.toInt - m1.toInt) + 1

    if (diff < 1) Seq() else if (sep == C.RANGE_DASH) genRangeListFromDash(y1.toInt, m1.toInt, carry, diff, delim)
    else genRangeListFromDots(y1, m1, y2, m2, delim)
  }

  def genRangeListFromDash(y: Int, m: Int, carry: Int, diff: Int, delim: String): PairSeq = {
    //    println(s"before: diff=$diff, carry=$carry, y=$y, m=$m, delim=$delim")

    val init_seq = Seq[(Int, Int)]()
    def gen(y: Int, m: Int, n: Int, s: Seq[(Int, Int)]): Seq[(Int, Int)] = {
      n match {

        case 1 => s :+ (y, m)
        case _ if m > carry => gen(y + 1, m - carry, n, s)
        case _ => gen(y, m + 1, n - 1, s :+ (y, m))
      }
    }

    val seq_tuple = gen(y, m, diff, init_seq)

    //    println(s"after size=${seq_tuple}.size")
    seq_tuple.toList.map(m => m._1.toString + delim + (if (delim == C.M) f"${m._2}%02d" else m._2))
      .map(m => (m, m))
  }

  def genRangeListFromDots(y1: String, m1: String, y2: String, m2: String, delim: String): PairSeq = {
    val s1 = y1 + delim + m1
    val s2 = y2 + delim + m2
    Seq((s1, s2))
  }

  def validRange(unit: String, num_str: String): Boolean = {
    val num = num_str.toInt
    unit match {
      case C.M if num > 0 && num < 13 => true
      case C.Q if num > 0 && num < 5 => true
      case _ => false
    }
  }

  // return a tuple like (2018M09, 2018Q3)
  def mq(): (String, String) = {
    val curr_ym = YearMonth.now
    val prev_ym = curr_ym.minusMonths(1)
    val last_month = prev_ym.toString.replace("-", C.M)

    val prev3_ym = curr_ym.minusMonths(3)
    val last_q = prev3_ym.getYear.toString + C.Q + prev3_ym.get(IsoFields.QUARTER_OF_YEAR).toString

    //    println(s"=== last_month: $last_month , last_q: $last_q")
    (last_month, last_q)
  }

  def getCurrDateTime(): LocalDateTime = {
    LocalDateTime.now()
  }

  def getTimeDiff(a: LocalDateTime, b: LocalDateTime): String = {
    val min_diff = MINUTES.between(a,b)
    val sec_diff = SECONDS.between(a,b)

    s"$min_diff mins, $sec_diff seconds"
  }
}