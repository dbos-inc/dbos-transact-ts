"use client";

import { useState, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Suspense } from 'react'

let intervalInitialized = false;

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
    const [isReconnecting, setIsReconnecting] = useState(false);

    const router = useRouter();
    const searchParams = useSearchParams();

    const startBackgroundJob = async () => {
      setIsRunning(true);
  
      setCurrentStep(0);

      let task = taskId
      if (taskId === "") {
        task = generateRandomString();
        setTaskid(task);
        updateQueryParam("id", task);
      }

      //  start the background job
      try {
        await fetch(`/tasks/${task}`, { method: "GET" });
      } catch (error) {
        console.error("Failed to start job", error);
        setIsRunning(false);
      }
  };

  const crashApp = async () => {

    if(!isRunning) {
        console.log("Not running, nothing to crash");
        return;
    }
    
    setIsRunning(false);
    
    console.log("Crashing the application");
    
    // stop the background job
    try {
      await fetch("/crash", { method: "GET" });
    } catch (error) {
      console.error("Failed to start job", error);
      
    }

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

  // fetch the current progress
  const fetchProgress = async () => {
    try {

      if (taskId === "") {
        console.log("No task to monitor");
        return;
      }

      const response = await fetch(`/step/${taskId}`, { method: "GET" });
      if (!response.ok) {
        console.error("Failed to fetch job progress", response.statusText);
        setIsReconnecting(true);
        return;
      }
      setIsReconnecting(false);

      const data = await response.json();

      console.log("Step completed", data.stepsCompleted);

      if (data.stepsCompleted) {
        setIsRunning(true);
        setCurrentStep(data.stepsCompleted);


        if (data.stepsCompleted === 10) {
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
      setIsReconnecting(true);
    }
  };

   // Polling the progress every 2 seconds while the job is running
  useEffect(() => {
    const idFromUrl = searchParams.get("id");
    if (idFromUrl) {
      setTaskid(idFromUrl);
      setIsRunning(true); // Assume the job is already running if there's an ID
    }
    
    if (!intervalInitialized) {
      const interval = setInterval(fetchProgress, 2000);
      intervalInitialized = true;
      return () => {
        clearInterval(interval);
        intervalInitialized = false;
      };
    }
  }, [searchParams]);


  return (
    <div>
      <div className="flex flex-row gap-2">  
        <button onClick={startBackgroundJob} disabled={isRunning} className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
          {isRunning ? "Job in Progress..." : "Start Background Job"}
        </button>
        <button onClick={crashApp} disabled={!isRunning} className="px-4 py-2 bg-red-500 text-white rounded hover:bg-red-600">
           { isRunning ? "Crash the application" : "Not Running"} 
        </button>
      </div>

      <p>
        {currentStep < 10
          ? `Your background task has completed step ${currentStep} of 10.`
          : "Background task completed successfully!"}
      </p>
      <p>
        {isReconnecting ? "Reconnecting..." : ""}
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
