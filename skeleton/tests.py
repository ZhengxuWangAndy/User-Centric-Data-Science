from explain_pipeline import *
import pytest

def test_task1():

    dataFolder = '../data/datasets/ADNI3/'


    data_util.split_csv(csv_file=dataFolder + 'ADNI3.csv',output_folder=dataFolder)

    bgLoader, testLoader = prepare_dataloaders(bg_csv=dataFolder + 'bg.csv',test_csv=dataFolder + 'test.csv')

    cnn = model._CNN(20,0.6)
    best = torch.load(dataFolder + 'cnn_best.pth',map_location=torch.device('cpu'))
    cnn.load_state_dict(best["state_dict"])
    cnn.eval()


    testMRIFilePath, testLabels = data_util.read_csv(dataFolder + 'test.csv')

    bgMRIFilePath, bgLabels = data_util.read_csv(dataFolder + 'bg.csv')


    logger.info("task1")

    # test of testdataset
    result = []
    for file in testLoader:
        with torch.no_grad():
            mri, path, label = file
            out = cnn(torch.unsqueeze(mri,1))
            result.append(0 if out[0][0] > out[0][1] else 1)
            
            

    correct = 0
    for i in range(len(testLabels)):
        if testLabels[i] == result[i]:
            correct += 1

    logger.info("accuracy of test data is " + str(correct/len(result)*100) + "%, wrong :" + str(len(result) - correct) + " total: " + str(len(result)))
    
    # test of bg dataset
    result_bg = []
    for file in bgLoader:
        with torch.no_grad():
            mri, path, label = file
            out = cnn(torch.unsqueeze(mri,1))
            result_bg.append(0 if out[0][0] > out[0][1] else 1)


    correct_bg = 0
    for i in range(len(bgLabels)):
        if bgLabels[i] == result_bg[i]:
            correct_bg += 1

    logger.info("accuracy of bg data is " + str(correct_bg/len(result_bg)*100) + "%, wrong :" + str(len(result_bg) - correct_bg) + " total: " + str(len(result_bg)))

    # df = pd.DataFrame({'Classified':['Correct','Incorrect'], 'Value':[correct + correct_bg, len(result_bg) + len(result) - correct - correct_bg]})

    assert 18 == correct + correct_bg