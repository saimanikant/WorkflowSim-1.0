package org.workflowsim.examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.MetaGetter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters.ClassType;


public class DessertationExample1{
	
	private static List<String> getAllVMNames() {

        SAXBuilder builder = new SAXBuilder();
        Document dom;


        try {
            dom = builder.build(new File("config/machines/machines.xml"));
            Element root = dom.getRootElement();
            List<Element> availableVMs = root.getChildren().get(0).getChildren("host");
            List<String> list = availableVMs.stream().map(v -> v.getAttribute("id").getValue()).collect(Collectors.toList());
            Collections.sort(list);
            return list;
        } catch (JDOMException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    protected static List<CondorVM> createVMs(int userId, int vms, long seed, List<LinkedHashMap<String, Object>> arr) {
        SAXBuilder builder = new SAXBuilder();

        Document dom;
        try {
            dom = builder.build(new File("config/machines/machines.xml"));
            Element root = dom.getRootElement();
            List<Element> availableVMs = root.getChildren().get(0).getChildren("host");

            LinkedList<CondorVM> results = new LinkedList<>();
            CondorVM[] vm = new CondorVM[vms];
            for (int i = 0; i < vms; ) {
                int randomNumber = (int) Math.round(MetaGetter.getRandomForCluster() * (availableVMs.size() - 1));

                Element selectedVM = availableVMs.get(randomNumber);
                double mips = 100;
                int ram = selectedVM.getChildren("prop").get(0).getAttribute("value").getIntValue();
                int pesNumber = selectedVM.getAttribute("core").getIntValue();

                vm[i] = new CondorVM(i, userId, mips, pesNumber, ram, 10000, 100000, "Xen", new CloudletSchedulerSpaceShared(arr));
                vm[i].setName(selectedVM.getAttribute("id").getValue()); 
                results.add(vm[i]);
                i++;
                // }
            }
            System.out.println(results.stream().map(v -> v.getName()).collect(Collectors.toList()));
            return results;

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    ////////////////////////// STATIC METHODS ///////////////////////

    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) throws IOException {
    	Log.printLine("========== OUTPUT ==========");
    	Log.printLine(String.join(",", getAllVMNames()));
    	
    	 if (args.length == 3) {
             MetaGetter.setWorkflow(args[0]);
             MetaGetter.setDistribution(args[1]);
             MetaGetter.setError(Double.parseDouble(args[2]));
             System.out.println("Start");
         }
    	 
    	 prepareSimulations(MetaGetter.getArr(), 1, 40);
    	
    }
    
    private static void prepareSimulations(List<LinkedHashMap<String, Object>> arr, int numberIterations, int clusterSize) throws IOException {

        //BufferedWriter resultsWriter = new BufferedWriter(new FileWriter("results_" + numberIterations + "_" + clusterSize + "_" + MetaGetter.getDistribution() + "_" + MetaGetter.getError() + "_" + MetaGetter.getWorkflow() + ".csv"));


        //resultsWriter.write("Workflow,Distribution,Error,NumberNodes,ClusterSeed,Scheduler,Runtime," + String.join(",", getAllVMNames()) + ",Nodes" + "\n");
        Long millis_start = System.currentTimeMillis();        
        for (long i = 0; i < numberIterations; i++) {
                                  
            runSimulation(i, Parameters.SchedulingAlgorithm.ROUNDROBIN, arr, clusterSize);
            MetaGetter.setListPointeroffset((int) (1000 * (i + 1)));
            MetaGetter.setRandPointerOffset((int) (1000 * (i + 1)));
            MetaGetter.resetGenerator();

        }
        System.out.println("Runtime in millis:" + (System.currentTimeMillis() - millis_start) + " for " + MetaGetter.getWorkflow() + "_" + MetaGetter.getDistribution() + "_" + MetaGetter.getError());
        //resultsWriter.close();
    }
    
    private static void runSimulation(Long seed, Parameters.SchedulingAlgorithm schedulingAlgorithm, List<LinkedHashMap<String, Object>> arr, int totalNumberVms) {
        try {
            // First step: Initialize the WorkflowSim package.
            /**
             * However, the exact number of vms may nft necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = totalNumberVms;//number of vms;
            /**
             * Should change this based on real physical path
             */
            String daxPath = "config/dax/" + MetaGetter.getWorkflow() + ".xml";;
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            Parameters.SchedulingAlgorithm sch_method;
            Parameters.PlanningAlgorithm pln_method;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;

            if (schedulingAlgorithm != Parameters.SchedulingAlgorithm.STATIC) {
                sch_method = schedulingAlgorithm;
                pln_method = Parameters.PlanningAlgorithm.INVALID;
            } else {
                sch_method = schedulingAlgorithm;
                pln_method = Parameters.PlanningAlgorithm.HEFT;
            }
            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */

            /**
             * No overheads
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            
            //System.out.print("HERE I AM");
            System.out.print(arr);
            //System.out.print("THis is me");
            List<CondorVM> vmlist0 = createVMs(wfEngine.getSchedulerId(0), Parameters.getVmNum(), seed, arr);
            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();            
            outputList0.forEach(System.out::println);
            
            CloudSim.stopSimulation();
            System.out.print(schedulingAlgorithm + "");
            printJobList(outputList0, schedulingAlgorithm, vmNum, seed, vmlist0);
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }
    
    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= 20; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = 4000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:

            for (int j = 0; j < 64; j++) {
                peList1.add(new Pe(j, new PeProvisionerSimple(mips)));
            }


            int hostId = 0;
            int ram = 256000; //host memory (MB)
            long storage = 10000000; //host storage
            int bw = 100000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))); // This is our first machine
            //hostId++;
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;        // the cost of using memory in this resource
        double costPerStorage = 0.1;    // the cost of using storage in this resource
        double costPerBw = 0.1;            // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();    //we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }
    
    protected static void printJobList(List<Job> list, Parameters.SchedulingAlgorithm schedulingAlgorithm, int numberVM, Long seed, List<CondorVM> vms) {

        TreeMap<String, Integer> map = new TreeMap<>();

        getAllVMNames().forEach(vm -> {
            map.put(vm, 0);
        });

        for (Job job : list) {
            for (CondorVM vm : vms) {
                if (job.getVmId() == vm.getId()) {
                    map.put(vm.getName(), map.get(vm.getName()) + 1);
                }
            }
        }        
        
        printJobList(list);
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
            Log.print(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("Stage-in");
            }
            for (Task task : job.getTaskList()) {
                Log.print(task.getCloudletId() + ",");
            }
            Log.print(indent);


            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {

                if(job.getTaskList().size() == 0) {
                    Log.print("SUCCESS");
                    Log.printLine(indent + indent +  job.getResourceId() +")"+ indent + indent + indent + job.getVmId()
                            + indent + indent + indent + dft.format(job.getActualCPUTime())
                            + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                            + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
                } else {
                    Log.print("SUCCESS");
                    Log.printLine(indent + indent + job.getTaskList().get(0).getType() +"(" + job.getResourceId() +")"+ indent + indent + indent + job.getVmId()
                            + indent + indent + indent + dft.format(job.getActualCPUTime())
                            + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                            + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
                }

            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }
    }
}