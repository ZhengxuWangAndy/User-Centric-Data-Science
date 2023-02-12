from assignment_12 import *
import pytest

f = '../data/friends.txt'
r = '../data/movie_ratings.txt'
uid = 1
mid = 1

# test scan
def test_Scan():
    scan = Scan(filepath=f,outputs=[],filter=[filter,uid,0])
    file = []
    file = scan.get_next()
    assert [1,1144] == file[0].tuple


# test select
def test_Select():
    scan = Scan(filepath=f,outputs=[],filter=[filter,uid,0])
    select = Select([scan],[],predicate,0,uid)

    result = []
    result = select.get_next()
    
    assert [1,1144] == result[0].tuple

# test join
def test_Join():
    F = Scan(filepath=f,outputs=[],filter=[filter,uid,0])
    R = Scan(filepath=r,outputs=[],filter=[filter,mid,1])
    join = Join([F],[R],[],1,0)

    test = []
    alltest = []
    while test != None:
        alltest += test
        test = join.get_next()

        # if test != None:
    assert 20 == len(alltest)


# test project
def test_Project():
    F = Scan(filepath=f,outputs=[])
    # R = Scan(filepath='./data/test_ratings.txt',outputs=[])
    project = Project([F],[],[0,1])
    test = []
    test = project.get_next()
    assert [1190,15] == test[0].tuple

# test groupby
def test_Groupby():
    R = Scan(filepath=r,outputs=[])
    grouby = GroupBy([R],[],1,2,AVG)
    test = []
    test = grouby.get_next()
    test.sort(key=lambda x : x.tuple[0])
    assert ['0',2.4515] == test[0].tuple

# test orderby
def test_Orderby():
    F = Scan(filepath=f,outputs=[])
    orderby = OrderBy([F],[],comparator,1,True)
    result = orderby.get_next()
    assert [926,0] == result[0].tuple

# test topk
def test_TopK():
    F = Scan(filepath=f,outputs=[])
    topk = TopK([F],[],5)
    result = topk.get_next()
    assert [1190,15] == result[0].tuple

def filter(line,id,index):
    return line[index] == str(id)



# # task 1:
def test_Task1():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)
    selectedR = Select([R],[],predicate,1,mid)
    joined = Join([selectedF],[selectedR],[],1,0)
    grouped = GroupBy([joined],[],3,4,AVG)
    # while True:
    temp = grouped.get_next()
    temp.sort(key=lambda x : x.tuple[0])
    assert ['1', 2.85] == temp[0].tuple

# task 2：
def test_Task2():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)
    joined = Join([selectedF],[R],[],1,0)
    projected = Project([joined],[],[3,4])
    grouped = GroupBy([projected],[],0,1,AVG)
    ordered = OrderBy([grouped],[],comparator,1,False)
    limited = TopK([ordered],[],1)
    projectResult = Project([limited],[],[0])
    result = projectResult.get_next()
    assert '3681' == result[0].tuple

# task3:
def test_Task3():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)
    selectedR = Select([R],[],predicate,1,mid)
    joined = Join([selectedF],[selectedR],[],1,0)
    projected= Project([joined],[],[2,4])
    histogtamed = Histogram([projected],[],2)

    temp = histogtamed.get_next()
    assert 6 == len(temp)

# task1 push
def test_Task1Push():
    sink = Sink(result=[])
    joined = Join([], [], [sink], 1, 0)
    selected_F = Select([], [joined],predicate,0,uid)
    joined.left_inputs = selected_F
    selected_R = Select([], [joined],predicate,1,mid)
    joined.right_inputs = selected_R
    F = Scan(filepath=f,outputs=[selected_F],filter=[filter,uid,0])
    R = Scan(filepath=r, outputs=[selected_R],filter=[filter,mid,1])
    F.start()
    R.start()
    assert [2.85] == AVG_push(sink.get_result(), 4)


# task2 push
def test_Task2Push():
    sink = Sink(result=[])
    projected = Project([],[sink],[0])
    limited = TopK([],[projected],1)
    ordered = OrderBy([],[limited],comparator,1,False)
    grouped = GroupBy([],[ordered],0,1,AVG)
    projected = Project([],[grouped],[3,4])
    joined = Join([], [], [projected], 1, 0)
    selected_F = Select([], [joined],predicate,0,uid)
    R = Scan(filepath=r, outputs=[joined])
    joined.right_inputs = R
    F = Scan(filepath=f,outputs=[selected_F])
    joined.left_inputs = selected_F
    F.start()
    R.start()

    tuple = sink.get_result()
    tuple.sort(key=lambda x : x.tuple[0])
    assert '0' == tuple[0].tuple

def test_Task3Push():
    sink = Sink(result=[])
    histogtamed = Histogram([],[sink],2)
    projected = Project([],[histogtamed],[2,4])
    joined = Join([],[],[projected],1,0)
    selectedF = Select([],[joined],predicate,0,uid)
    joined.left_inputs = selectedF
    selectedR = Select([],[joined],predicate,1,mid)
    joined.right_inputs = selectedR
    F = Scan(filepath=f,outputs=[selectedF])
    R = Scan(filepath=r, outputs=[selectedR])
    F.start()
    R.start()
    assert (4, 3) == sink.get_result()[0]



# if __name__ == "__main__": 
#     assert [1190,15] == testScan()
#     assert [1190,15] == testSelect()
#     assert 170 == testJoin()
#     assert [1190,15] == testProject()
#     assert ['2',5.25] == testGroupby()
#     assert [6,0] == testOrderby()
#     assert [1190,15] == testTopK()
    # assert ['1', 2.85] == testTask1()
    # assert '6467' == testTask2()
    # # assert [('4', 5), ('3', 2), ('0', 2), ('5', 1)] == testTask3() # each time has different order, affect the result.
    # assert [2.08] == testTask1Push()
    # assert (4, 3) == testTask3Push()
    