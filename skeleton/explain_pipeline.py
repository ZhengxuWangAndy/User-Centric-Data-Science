import argparse
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import nibabel as nib
import numpy as np
import os
import shap
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import data_util
import model
import pandas as pd
import argparse

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)



# This is a color map that you can use to plot the SHAP heatmap on the input MRI
colors = []
for l in np.linspace(1, 0, 100):
    colors.append((30./255, 136./255, 229./255,l))
for l in np.linspace(0, 1, 100):
    colors.append((255./255, 13./255, 87./255,l))
red_transparent_blue = LinearSegmentedColormap.from_list("red_transparent_blue", colors)


# Returns two data loaders (objects of the class: torch.utils.data.DataLoader) that are
# used to load the background and test datasets.
def prepare_dataloaders(bg_csv, test_csv, bg_batch_size = 1, test_batch_size= 1, num_workers=1):
    '''
    Attributes:
        bg_csv (str): The path to the background CSV file.
        test_csv (str): The path to the test data CSV file.
        bg_batch_size (int): The batch size of the background data loader
        test_batch_size (int): The batch size of the test data loader
        num_workers (int): The number of sub-processes to use for dataloader
    '''
    # YOUR CODE HERE
    bgData = data_util.CNN_Data(bg_csv)
    testData = data_util.CNN_Data(test_csv)

    bgLoader = DataLoader(dataset=bgData, batch_size=bg_batch_size)
    testLoader = DataLoader(dataset=testData, batch_size=test_batch_size)

    return bgLoader, testLoader

# Generates SHAP values for all pixels in the MRIs given by the test_loader
def create_SHAP_values(cnn, bg_loader, test_loader, mri_count=5, save_path=''):
    '''
    Attributes:
        bg_loader (torch.utils.data.DataLoader): Dataloader instance for the background dataset.
        test_loader (torch.utils.data.DataLoader): Dataloader instance for the test dataset.
        mri_count (int): The total number of explanations to generate.
        save_path (str): The path to save the generated SHAP values (as .npy files).
    '''
    # YOUR CODE HERE
    mri, path, label = next(iter(bg_loader))
    mri = torch.unsqueeze(mri,1)

    shapExplainer = shap.DeepExplainer(model=cnn, data=mri)

    if not os.path.exists(save_path + '/SHAP/'):
        os.makedirs(save_path + '/SHAP/')

    for test_mri,test_path,test_label in test_loader:
        test_mri = torch.unsqueeze(test_mri, 1)
        shapValues = np.array(shapExplainer.shap_values(test_mri))

        np.save(file=save_path + '/SHAP/' + test_path[0], arr=shapValues)
        





# Aggregates SHAP values per brain region and returns a dictionary that maps 
# each region to the average SHAP value of its pixels. 
def aggregate_SHAP_values_per_region(shap_values, seg_path, AD):
    '''
    Attributes:
        shap_values (ndarray): The shap values for an MRI (.npy).
        seg_path (str): The path to the segmented MRI (.nii). 
        brain_regions (dict): The regions inside the segmented MRI image (see data_utl.py)
    '''
    # YOUR CODE HERE
    shapDict = {}
    seg = nib.load(seg_path)
    map = seg.get_fdata()
    countMap = {}
    if AD == 0:
        shap_values = shap_values[0][0][0]
    else:
        shap_values = shap_values[1][0][0]

    for i in range(len(shap_values)):
        for j in range(len(shap_values[0])):
            for k in range(len(shap_values[0][0])):
                if shap_values[i][j][k] != 0:
                    if map[i][j][k] not in shapDict.keys():
                        shapDict[map[i][j][k]] = 0
                        countMap[map[i][j][k]] = 0
                    else:
                        shapDict[map[i][j][k]] += shap_values[i][j][k]
                        countMap[map[i][j][k]] += 1

    for key in shapDict.keys():
        shapDict[key] = shapDict[key] / countMap[key]

    return shapDict







# Returns a list containing the top-10 most contributing brain regions to each predicted class (AD/NotAD).
def output_top_10_lst(map, csv_file, brain_regions):
    '''
    Attribute:
        csv_file (str): The path to a CSV file that contains the aggregated SHAP values per region.
    '''
    # YOUR CODE HERE
    lis = sorted(map.items(), key = lambda x:x[1], reverse=True)
    regions = []

    for key in lis[:10]:
        regions.append(brain_regions[key[0]])

    df = pd.DataFrame({'Region number':[item[0] for item in lis[:10]],'region': regions, 'value':[item[1] for item in lis[:10]]})
    df.to_csv(csv_file)

# Plots SHAP values on a 2D slice of the 3D MRI. 
def plot_shap_on_mri(subject_mri, shap_values, label):
    '''
    Attributes:
        subject_mri (str): The path to the MRI (.npy).
        shap_values (str): The path to the SHAP explanation that corresponds to the MRI (.npy).
    '''
    # YOUR CODE HERE
    mri = np.load(subject_mri)
    mri = torch.unsqueeze(torch.tensor(mri),0)
    mri = np.array(torch.unsqueeze(mri,-1))

    SHAP = np.load(shap_values)
    SHAP = SHAP[label]
    SHAP = torch.squeeze(torch.tensor(SHAP),0)
    SHAP = np.array(torch.unsqueeze(SHAP,-1))

    if label == 0:
        if not os.path.exists(args.outputFolder+'NC/'):
            os.makedirs(args.outputFolder+'NC/')

        
        image = shap.image_plot(shap_values = SHAP[:,80],pixel_values = mri[:,80], show = False)
        plt.savefig(args.outputFolder+'NC/task3-1.png')

        image = shap.image_plot(shap_values = SHAP[:,:,80],pixel_values = mri[:,:,80], show = False)
        plt.savefig(args.outputFolder+'NC/task3-2.png')

        image = shap.image_plot(shap_values = SHAP[:,:,:,80],pixel_values = mri[:,:,:,80], show = False)
        plt.savefig(args.outputFolder+'NC/task3-3.png')

    else:
        if not os.path.exists(args.outputFolder+'AD/'):
            os.makedirs(args.outputFolder+'AD/')

        image = shap.image_plot(shap_values = SHAP[:,80],pixel_values = mri[:,80], show = False)
        plt.savefig(args.outputFolder+'AD/task3-1.png')

        image = shap.image_plot(shap_values = SHAP[:,:,80],pixel_values = mri[:,:,80], show = False)
        plt.savefig(args.outputFolder+'AD/task3-2.png')

        image = shap.image_plot(shap_values = SHAP[:,:,:,80],pixel_values = mri[:,:,:,80], show = False)
        plt.savefig(args.outputFolder+'AD/task3-3.png')




if __name__ == '__main__':

# test querys:
# python3 explain_pipeline.py --task 1 --dataFolder ../data/datasets/ADNI3/ --outputFolder ../output/

# python3 explain_pipeline.py --task 2 --dataFolder ../data/datasets/ADNI3/ --outputFolder ../output/

# python3 explain_pipeline.py --task 3 --dataFolder ../data/datasets/ADNI3/ --outputFolder ../output/

# python3 explain_pipeline.py --task 4 --dataFolder ../data/datasets/ADNI3/ --outputFolder ../output/


    # TASK I: Load CNN model and isntances (MRIs)
    #         Report how many of the 19 MRIs are classified correctly
    # YOUR CODE HERE

    parser = argparse.ArgumentParser()


    parser.add_argument("--task")
    parser.add_argument("--dataFolder")
    parser.add_argument("--outputFolder")
    args = parser.parse_args()


    data_util.split_csv(csv_file=args.dataFolder + 'ADNI3.csv',output_folder=args.dataFolder)

    bgLoader, testLoader = prepare_dataloaders(bg_csv=args.dataFolder + 'bg.csv',test_csv=args.dataFolder + 'test.csv')

    cnn = model._CNN(20,0.6)
    best = torch.load(args.dataFolder + 'cnn_best.pth',map_location=torch.device('cpu'))
    cnn.load_state_dict(best["state_dict"])
    cnn.eval()


    testMRIFilePath, testLabels = data_util.read_csv(args.dataFolder + 'test.csv')

    bgMRIFilePath, bgLabels = data_util.read_csv(args.dataFolder + 'bg.csv')

    logger.info("Assignment 3")

    if args.task == '1':
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

        df = pd.DataFrame({'Classified':['Correct','Incorrect'], 'Value':[correct + correct_bg, len(result_bg) + len(result) - correct - correct_bg]})
        df.to_csv(args.outputFolder + "task-1.csv")

    # TASK II: Probe the CNN model to generate predictions and compute the SHAP 
    #          values for each MRI using the DeepExplainer or the GradientExplainer. 
    #          Save the generated SHAP values that correspond to instances with a
    #          correct prediction into output/SHAP/data/
    # YOUR CODE HERE 


    if args.task == '2':
        logger.info("task2")

        create_SHAP_values(cnn, bgLoader, testLoader, save_path=args.outputFolder)



    # TASK III: Plot an explanation (pixel-based SHAP heatmaps) for a random MRI. 
    #           Save heatmaps into output/SHAP/heatmaps/
    # YOUR CODE HERE 


    if args.task == '3':
        logger.info("task3")

        plot_shap_on_mri('..' + testMRIFilePath[0], args.outputFolder + 'SHAP/' + testMRIFilePath[0].split('/')[-1], testLabels[0])
        plot_shap_on_mri('..' + testMRIFilePath[1], args.outputFolder + 'SHAP/' + testMRIFilePath[1].split('/')[-1], testLabels[1])

    # TASK IV: Map each SHAP value to its brain region and aggregate SHAP values per region.
    #          Report the top-10 most contributing regions per class (AD/NC) as top10_{class}.csv
    #          Save CSV files into output/top10/
    # YOUR CODE HERE 

    if args.task == '4':

        logger.info("task4")

        SHAP = np.load(args.outputFolder + 'SHAP/' + 'ADNI_135_S_6509_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190806143145652_41_S847764_I1195531.npy')

        shapDict = aggregate_SHAP_values_per_region(SHAP, args.dataFolder + 'seg/ADNI_135_S_6509_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190806143145652_41_S847764_I1195531.nii',0)

        output_top_10_lst(shapDict, args.outputFolder + 'task-4-False.csv', brain_regions=data_util.brain_regions)



        SHAP = np.load(args.outputFolder + 'SHAP/' + 'ADNI_022_S_6013_MR_Sagittal_3D_Accelerated_MPRAGE_br_raw_20190314145101831_129_S806245_I1142379.npy')

        shapDict = aggregate_SHAP_values_per_region(SHAP, args.dataFolder + 'seg/ADNI_022_S_6013_MR_Sagittal_3D_Accelerated_MPRAGE_br_raw_20190314145101831_129_S806245_I1142379.nii',1)

        output_top_10_lst(shapDict, args.outputFolder + 'task-4-True.csv', brain_regions=data_util.brain_regions)
