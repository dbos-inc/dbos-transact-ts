"use client";

import { useState, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { dbosBackgroundTask } from "@/actions/dbosWorkflow";
import { Suspense } from 'react'

function generateRandomString(): string {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const array = new Uint8Array(6);
    crypto.getRandomValues(array); // Fills the array with cryptographically random values

    return Array.from(array)
        .map(x => chars[x % chars.length])
        .join('');
}

function BackGroundTask() {
    const [isRunning, setIsRunning] = useState(false);
    const [currentStep, setCurrentStep] = useState(0);
    const [taskId, setTaskid] = useState("");

    const router = useRouter();
    const searchParams = useSearchParams();


      // Function to start the background job
  const startBackgroundJob = async () => {
    setIsRunning(true);
    setCurrentStep(0);

    let task = taskId
    if (taskId === "") {
        task = generateRandomString();
        setTaskid(task);
        updateQueryParam("id", task);
    }


    // Simulate calling a REST API to start the background job
    try {
      await fetch(`/tasks/${task}`, { method: "GET" });
      //   await dbosBackgroundTask();
    } catch (error) {
      console.error("Failed to start job", error);
      setIsRunning(false);
    }
  };

  const crashApp = async () => {

    console.log("Crashing the application");
    
    // Simulate calling a REST API to start the background job
    try {
      await fetch("/crash", { method: "GET" });
      //   await dbosBackgroundTask();
    } catch (error) {
      console.error("Failed to crash job", error);
      
    }
    setIsRunning(false);
  };

   // Update the URL query parameter
   const updateQueryParam = (key: string, value: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set(key, value);
    const newUrl = `${window.location.pathname}?${params.toString()}`;
    router.replace(newUrl);
  };

  // Remove the `id` query parameter from the URL
  const clearQueryParam = (key: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.delete(key);
    const newUrl = `${window.location.pathname}?${params.toString()}`;
    router.replace(newUrl);
  };


  // Function to fetch the current progress
  const fetchProgress = async () => {
    try {

      if (taskId === "") {
        console.log("No task to monitor");
        return;
      }

      const response = await fetch(`/step/${taskId}`, { method: "GET" });
      const data = await response.json();

      if (!isRunning) {
        setIsRunning(true);
      }

      console.log(data);
      if (data.stepsCompleted) {
        setCurrentStep(data.stepsCompleted);

        if (data.stepsCompleted === 9) {
            clearQueryParam("id");  
          setIsRunning(false);
          setTaskid("");
          setCurrentStep(0);
          
        }
      }
    } catch (error) {
      console.error("Failed to fetch job progress", error);
      setIsRunning(false);
        setTaskid("");
        // setCurrentStep(0);
    }
  };

   // Polling the progress every 2 seconds while the job is running
   useEffect(() => {
    const idFromUrl = searchParams.get("id");
    if (idFromUrl) {
      setTaskid(idFromUrl);
      setIsRunning(true); // Assume the job is already running if there's an ID
    }
    if (isRunning) {
      const interval = setInterval(fetchProgress, 2000);
      return () => clearInterval(interval);
    }
    console.log("isRunning: ", isRunning);
  }, [isRunning, searchParams]);

  console.log(`isRunning: ${isRunning}, currentStep: ${currentStep}`);

  return (
    <div>


      <div className="flex flex-row gap-2">  
      <button onClick={startBackgroundJob} disabled={isRunning} className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
        {isRunning ? "Job in Progress..." : "Start Background Job"}
      </button>
      <button onClick={crashApp} disabled={!isRunning}
            id="crash-button"
            className="bg-red-600 hover:bg-red-700 text-white font-semibold py-2 px-4 rounded shadow transition duration-150 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-opacity-50 disabled:opacity-50 disabled:cursor-not-allowed disabled:transform-none disabled:hover:bg-red-600"        >
            Crash the application
      </button>
      </div>

      <p>
        {currentStep < 10
          ? `Your background task has completed step ${currentStep} of 9.`
          : "Background task completed successfully!"}
      </p>

    </div>
  );

}

const WrappedBackgroundJobComponent = () => (
    <Suspense fallback={<p>Loading...</p>}>
      <BackGroundTask />
    </Suspense>
  );
  
  export default WrappedBackgroundJobComponent;
