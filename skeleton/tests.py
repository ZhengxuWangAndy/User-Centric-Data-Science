from unittest import result
from assignment_12 import *
import pytest

f = '../data/friends_grader.txt'
r = '../data/movie_ratings_grader.txt'
uid = 1
mid = 1

def test_Assignment2Task1():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)
    joined = Join([selectedF],[R],[],1,0)
    projected = Project([joined],[],[3,4])
    grouped = GroupBy([projected],[],0,1,AVG)
    ordered = OrderBy([grouped],[],comparator,1,False)
    limited = TopK([ordered],[],1)
    projectResult = Project([limited],[],[0,1])
    result = projectResult.get_next()
    lineage = result[0].lineage()
    logger.info(result[0].tuple)

    lineages = list(set([tuple(x.tuple) for x in lineage]))
    logger.info(lineages)
    assert 16 == len(lineages)


def test_Assignment2Task2():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)    # uid = 1
    selectedR = Select([R],[],predicate,1,mid)    # mid = 1
    joined = Join([selectedF],[selectedR],[],1,0)
    grouped = GroupBy([joined],[],3,4,AVG)
    # while True:
    temp = grouped.get_next()

    logger.info(temp[0].tuple)
    where = temp[0].where(att_index = 0)
    logger.info(where)
    assert 4 == len(where)


def test_Assignment2Task3():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)
    joined = Join([selectedF],[R],[],1,0)
    projected = Project([joined],[],[3,4])
    grouped = GroupBy([projected],[],0,1,AVG)
    ordered = OrderBy([grouped],[],comparator,1,False)
    limited = TopK([ordered],[],1)
    projectResult = Project([limited],[],[0,1])
    result = projectResult.get_next()

    logger.info(result[0].tuple)
    
    temp = str(result[0].metadata)
    temp = re.sub("[\\\[\]'\"]", '', temp)
    logger.info(temp)
    assert 52 == len(temp)


def testAssignment2Task4():
    F = Scan(filepath=f, outputs=[])
    R = Scan(filepath=r, outputs=[])
    selectedF = Select([F],[],predicate,0,uid)
    joined = Join([selectedF],[R],[],1,0)
    projected = Project([joined],[],[3,4])
    grouped = GroupBy([projected],[],0,1,AVG)
    ordered = OrderBy([grouped],[],comparator,1,False)
    limited = TopK([ordered],[],1)
    projectResult = Project([limited],[],[0,1])
    result = projectResult.get_next()

    logger.info(result[0].tuple)    
    respons = result[0].responsible_inputs()
    logger.info(respons)

    assert 2 == len(respons)


            
def test_Assignment2Task1_push():
 


    sink = Sink(result=[])
    projected = Project([],[sink],[0])
    limited = TopK([],[projected],1)
    ordered = OrderBy([],[limited],comparator,1,False)
    grouped = GroupBy([],[ordered],0,1,AVG)
    projected2 = Project([],[grouped],[3,4])
    joined = Join([], [], [projected2], 1, 0)
    selected_F = Select([], [joined],predicate,0,uid)
    R = Scan(filepath=r, outputs=[joined])
    joined.right_inputs = [R]
    F = Scan(filepath=f,outputs=[selected_F])
    joined.left_inputs = [selected_F]

    projected.outputs.append(limited)
    limited.outputs.append(ordered)
    ordered.outputs.append(grouped)
    grouped.outputs.append(projected2)
    projected2.outputs.append(joined)
    joined.outputs.append(selected_F)
    selected_F.outputs.append(F)
    F.start()
    R.start()

    lineages = set()
    for temp in sink.get_result():
        logger.info(temp.tuple)
        for lineage in temp.lineage():
            lineages.add(tuple(lineage.tuple))
    logger.info(list(lineages))
    assert 16 == len(list(lineages))


def test_Assignment2Task2_push():
    sink = Sink(result=[])
    grouped = GroupBy([], [sink], 0, 4, AVG, track_prov=True)
    joined = Join([], [], [grouped], 1, 0)
    selected_F = Select([], [joined],predicate,0,uid)
    joined.left_inputs = [selected_F]
    selected_R = Select([], [joined],predicate,1,mid)
    joined.right_inputs = [selected_R]
    F = Scan(filepath=f,outputs=[selected_F],filter=[filter,uid,0])
    R = Scan(filepath=r, outputs=[selected_R],filter=[filter,mid,1])
    grouped.outputs.append(joined)
    joined.outputs.append(selected_F)
    selected_F.outputs.append(F)
    selected_R.outputs.append(R)
    F.start()
    R.start()
    result = sink.get_result()

    logger.info(result[0].tuple)

    wheres = result[0].where(att_index=0)
    logger.info(sorted(wheres))
    assert 4 == len(wheres)



def test_Assignment2Task3_push():
    sink = Sink(result=[])
    projected = Project([],[sink],[0])
    limited = TopK([],[projected],1)
    ordered = OrderBy([],[limited],comparator,1,False)
    grouped = GroupBy([],[ordered],0,1,AVG)
    projected2 = Project([],[grouped],[3,4])
    joined = Join([], [], [projected2], 1, 0)
    selected_F = Select([], [joined],predicate,0,uid)
    R = Scan(filepath=r, outputs=[joined])
    joined.right_inputs = [R]
    F = Scan(filepath=f,outputs=[selected_F])
    joined.left_inputs = [selected_F]

    projected.outputs.append(limited)
    limited.outputs.append(ordered)
    ordered.outputs.append(grouped)
    grouped.outputs.append(projected2)
    projected2.outputs.append(joined)
    joined.outputs.append(selected_F)
    selected_F.outputs.append(F)
    F.start()
    R.start()

    for temp in sink.get_result():
        logger.info(temp.tuple)
        temp = str(temp.metadata)
        temp = re.sub("[\\\[\]'\"]", '', temp)
        logger.info(temp)
        assert 52 == len(temp)



def test_Assignment2Task4_push():
    sink = Sink(result=[])
    projected = Project([],[sink],[0])
    limited = TopK([],[projected],1)
    ordered = OrderBy([],[limited],comparator,1,False)
    grouped = GroupBy([],[ordered],0,1,AVG)
    projected2 = Project([],[grouped],[3,4])
    joined = Join([], [], [projected2], 1, 0)
    selected_F = Select([], [joined],predicate,0,uid)
    R = Scan(filepath=r, outputs=[joined])
    joined.right_inputs = [R]
    F = Scan(filepath=f,outputs=[selected_F])
    joined.left_inputs = [selected_F]

    projected.outputs.append(limited)
    limited.outputs.append(ordered)
    ordered.outputs.append(grouped)
    grouped.outputs.append(projected2)
    projected2.outputs.append(joined)
    joined.outputs.append(selected_F)
    selected_F.outputs.append(F)
    F.start()
    R.start()

    for temp in sink.get_result():
        logger.info(temp.tuple)
        result = temp.responsible_inputs()
        logger.info(result)
        assert 2 == len(result)
