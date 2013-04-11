/*
 * 
 *  Copyright 2012-2013 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package org.workflowsim.clustering.balancing.metrics;

import org.workflowsim.clustering.balancing.metrics.BalancingMetric;
import org.workflowsim.clustering.TaskSet;
import java.util.ArrayList;

/**
 *
 * @author Weiwei Chen
 */
public class PipelineRuntimeVariance implements BalancingMetric{
    @Override
    public double getMetric(ArrayList<TaskSet> list){
        if(list == null || list.size() <= 1){
            return 0.0;
        }
        double[] rv = new double[list.size()];
        
        for(int i = 0; i < list.size();i++){
            TaskSet set = (TaskSet)list.get(i);
            rv[i] = getPipelineSum(set);
            //Log.printLine(rv[i]);
        }
        double sum = 0.0;
        for(int i = 0; i < list.size(); i++){
            sum += rv[i];
        }
        double mean = sum / list.size();
        sum = 0.0;
        for(int i = 0; i < list.size(); i++){
            sum += Math.pow(rv[i] - mean, 2);
        }
        if(mean == 0.0) {
            return 0.0;
        }
        return Math.sqrt(sum / list.size()) / mean;
    }
     private double getPipelineSum(TaskSet task){
        double sum = 0.0;
        if(task == null){
            return sum;
        }
        sum += task.getJobRuntime();
        while(task.getChildList().size() == 1){
            TaskSet kid  = task.getChildList().get(0);
            if(kid.getParentList().size()==1){
                sum += kid.getJobRuntime();
            }else{
                break;
            }
            task = kid;
        }
        return sum;
    }
}