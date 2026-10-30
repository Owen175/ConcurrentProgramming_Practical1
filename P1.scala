import ox.scl._

object P1 {
    def main(args: Array[String]): Unit = {
        comparatorTest(10)
        sort4Test()
        insertTest(10)
        insertionSortTest(10)
    }

    def comparator(in0: ??[Int], in1: ??[Int], out0: !![Int], out1: !![Int]): ThreadGroup = thread {
        var i0, i1, temp = 0
        repeat {  // Loops until a message cannot be sent/recieved. 
            i0 = 0; i1 = 0
            run(thread {i0 = in0?()} || thread {i1 = in1?()})  // Gets inputs from in0 and in1 in either order
            if (i0 > i1) {
                temp = i0
                i0 = i1
                i1 = temp
            }  // Sets i0 to be the smaller one and i1 to the larger one
            run (thread {out0!i0} || thread {out1!i1})  // Outputs to out0 and out1 in either order. 
        }
        in0.close
        in1.close
        out0.endOfStream
        out1.endOfStream
        // Cleanly closes the channels in case one left open
    }
    def sort4(ins: List[??[Int]], outs: List[!![Int]]) : ThreadGroup = {
        require(ins.length == 4 && outs.length == 4)
        val currentOuts = new Array[SyncChan[Int]](4)
        currentOuts(0) = new SyncChan[Int]; currentOuts(2) = new SyncChan[Int]
        var outGroup = comparator(ins(0),ins(2),currentOuts(0),currentOuts(2))
        currentOuts(1) = new SyncChan[Int]; currentOuts(3) = new SyncChan[Int]
        outGroup = outGroup || comparator(ins(1),ins(3),currentOuts(1),currentOuts(3))

        var temp1 = currentOuts(0); var temp2 = currentOuts(1)
        currentOuts(1) = new SyncChan[Int]
        outGroup = outGroup || comparator(temp1,temp2,outs(0),currentOuts(1))
        temp1 = currentOuts(2); temp2 = currentOuts(3)
        currentOuts(2) = new SyncChan[Int]
        outGroup = outGroup || comparator(temp1,temp2,currentOuts(2),outs(3))

        temp1 = currentOuts(1); temp2 = currentOuts(2)
        outGroup = outGroup || comparator(temp1,temp2,outs(1),outs(2))

        outGroup
    }

    def insert(ins: List[??[Int]], in: ??[Int], outs: List [!![Int]]) : ThreadGroup = {
        val n = ins.length; require(n >= 1 && outs.length == n+1)
        val in00, in01, out00, out01 = new SyncChan[Int]
        val c0 = comparator(in00, in01, out00, out01)
        if (n == 1) {  // Simple n=1 case - one comparison
            c0 || thread {
                in01!(in?()); in00!(ins(0)?())  
                outs(0)!(out00?()); outs(1)!(out01?())
                outs(0).endOfStream
                outs(1).endOfStream
                in00.endOfStream
                in01.endOfStream
            }
            
        }
        else {
            var newIns: List[SyncChan[Int]] = Nil
            var newOuts: List[SyncChan[Int]] = Nil
            val newIn = new SyncChan[Int]
            for (i <- 0 until n-1) {
                newIns = (new SyncChan[Int]) :: newIns
                newOuts = (new SyncChan[Int]) :: newOuts
            }
            newOuts = (new SyncChan[Int]) :: newOuts
            // Constructed a list of newIns and newOuts one shorter than the original ones. 
            c0 || thread{ // Sorts in and ins(0)'s message and puts the smaller as out(0) and the larger as the new in. 
                in01!(in?()); in00!(ins(0)?())
                outs(0)!(out00?()); newIn!(out01?())
                in00.endOfStream
                in01.endOfStream
                outs(0).endOfStream
                newIn.endOfStream
            } || thread { // Populates newIns
                for (i <- 0 until n-1) {
                    newIns(i)!(ins(i+1)?())
                    newIns(i).endOfStream
                }
            } || insert(newIns, newIn, newOuts) || thread {  // Relies on the recursion to populate outs
                for (i <- 0 until n) {
                    outs(i+1)!(newOuts(i)?())
                    outs(i+1).endOfStream
                }
            }
            insert(newOuts,
            
        }
    }


    def sender(out: !![Int], x:Int) = thread{
        out!(x)
        out.endOfStream
    }
    def receiver(ins: List[??[Int]], xs: Array[Int]) = thread{
        val x0 = ins(0)?(); val x1 = ins(1)?(); val x2 = ins(2)?(); val x3 = ins(3)?()
        val doneArr = Array[Int](x0,x1,x2,x3)
        //println(x0);println(x1);;println(x2);println(x3);
        assert(xs.sorted.sameElements(doneArr))
    } 

    def insertionSort(ins: List[??[Int]], outs: List[!![Int]]) : ThreadGroup = {
        val n = ins.length; require(n >= 2 && outs.length == n)
        
        if (n == 2) { // Just sorting 2 and assigning to outs
            val in00, in01, out00, out01 = new SyncChan[Int]
            val c0 = comparator(in00, in01, out00, out01)
            c0 || thread {
                in01!(ins(0)?()); in00!(ins(1)?())  
                outs(0)!(out00?()); outs(1)!(out01?())
                outs(0).endOfStream
                outs(1).endOfStream
                in00.endOfStream
                in01.endOfStream
            }
        }
        else {
            var newIns: List[SyncChan[Int]] = Nil  // Creating the new ins and outs
            var newOuts: List[SyncChan[Int]] = Nil
            for (i <- 1 until n) {
                newIns = (new SyncChan[Int]) :: newIns
                newOuts = (new SyncChan[Int]) :: newOuts
            }
            
            // Insert consumes ins(0), so must be first
            insert(newOuts, ins(0), outs) || thread {
                for (i <- 1 until n) {
                    newIns(i-1)!(ins(i)?())
                    newIns(i-1).endOfStream

                }
            } || insertionSort(newIns, newOuts)
        }
    }

    def comparatorTest(n: Int): Unit = {
        val rand = new scala.util.Random
        var y1, y2 = 0
        for (i <- 0 until n) { 
            val in0, in1, out0, out1 = new SyncChan[Int]
            val cp = comparator(in0, in1, out0, out1)

            val x1 = rand.nextInt; val x2 = rand.nextInt
            run(thread {in0!x1; in1!x2; in0.endOfStream; in1.endOfStream} || cp || thread { y1 = out0?(); y2 = out1?();  out0.close; out1.close})
            assert(y1<=y2)

        }
        print("Test complete")
    }
    def sort4Test() = {
        val rand = new scala.util.Random

        for (_ <- 0 until 1000){
            val max = 100
            val xs = Array.fill(4)(rand.nextInt(max))
            val ins = List.fill(4)(new SyncChan[Int])
            val outs = List.fill(4)(new SyncChan[Int])
            val testThread = sender(ins(0),xs(0)) || sender(ins(1),xs(1)) || sender(ins(2),xs(2)) || sender(ins(3),xs(3)) || sort4(ins, outs) || receiver(outs, xs)
            run(testThread)
        }
        println("Tests complete")
    }



    def insertTest(n: Int): Unit = {
        // Initialising test variables
        val rand = new scala.util.Random
        var tasksize = 0; var testList: List[Int] = Nil; var testIns: List[SyncChan[Int]] = Nil; var testOuts: List[SyncChan[Int]] = Nil
        var sortList: List[Int] = Nil
        var sortListWithInsert: List[Int] = Nil
        var inserted = 0
        var testIn = new SyncChan[Int]

        for (i <- 0 until n) {
            tasksize = rand.nextInt(40) + 2
            //println("Test " + (i+1) + " starting, with a list of length " + tasksize)
            testIn = new SyncChan[Int]
            testList = Nil; testIns = Nil; testOuts = Nil
            for (j <- 0 until tasksize-1) {  
                testList = rand.nextInt :: testList
                testIns = (new SyncChan[Int]) :: testIns
                testOuts = (new SyncChan[Int]) :: testOuts
            }
            inserted = rand.nextInt
            testOuts = (new SyncChan[Int]) :: testOuts
            sortList = testList.sorted
            sortListWithInsert = (inserted :: sortList).sorted
            run(  // running a singular test
                thread {testIn!inserted; testIn.endOfStream} || thread {
                    for (j <- 0 until tasksize-1) {
                        testIns(j)!sortList(j)
                        testIns(j).endOfStream
                    }
                } || insert(testIns, testIn, testOuts) || thread {
                    for (j <- 0 until tasksize) {
                        if (sortListWithInsert(j) != testOuts(j)?()) println("Error found")
                    }
                }
            )
        }
        println("Tests finished")

    }

    def insertionSortTest(n: Int): Unit = {
        val rand = new scala.util.Random
        var tasksize = 0; var testList: List[Int] = Nil; var testIns: List[SyncChan[Int]] = Nil; var testOuts: List[SyncChan[Int]] = Nil
        var sortList: List[Int] = Nil
        for (i <- 0 until n) {
            tasksize = rand.nextInt(40) + 2
            // println("Test " + (i+1) + " starting, with a list of length " + tasksize)

            testList = Nil; testIns = Nil; testOuts = Nil
            for (j <- 0 until tasksize) {
                testList = rand.nextInt :: testList
                testIns = (new SyncChan[Int]) :: testIns
                testOuts = (new SyncChan[Int]) :: testOuts
            }
            sortList = testList.sorted

            run(
                thread {
                    for (j <- 0 until tasksize) {
                        testIns(j)!testList(j)
                        testIns(j).endOfStream

                    }
                } || insertionSort(testIns, testOuts) || thread {
                    for (j <- 0 until tasksize) {
                        if (sortList(j) != testOuts(j)?()) println("Error found") 
                    }
                }
            )
        }
    println("Tests finished")
    }
}
