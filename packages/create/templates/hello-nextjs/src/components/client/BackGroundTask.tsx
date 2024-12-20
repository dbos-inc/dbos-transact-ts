"use client";

import { useState, useEffect } from "react";
import { dbosBackgroundTask } from "@/actions/dbosWorkflow";

function generateRandomString(): string {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const array = new Uint8Array(6);
    crypto.getRandomValues(array); // Fills the array with cryptographically random values

    return Array.from(array)
        .map(x => chars[x % chars.length])
        .join('');
}

export default function BackGroundTask() {
    const [isRunning, setIsRunning] = useState(false);
    const [currentStep, setCurrentStep] = useState(0);
    const [taskId, setTaskid] = useState("");


      // Function to start the background job
  const startBackgroundJob = async () => {
    setIsRunning(true);
    setCurrentStep(0);

    let task = generateRandomString();
    setTaskid(task);

    // Simulate calling a REST API to start the background job
    try {
      await fetch(`/tasks/${task}`, { method: "GET" });
      //   await dbosBackgroundTask();
    } catch (error) {
      console.error("Failed to start job", error);
      setIsRunning(false);
    }
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

      console.log(data);
      if (data.stepsCompleted) {
        setCurrentStep(data.stepsCompleted);

        if (data.stepsCompleted === 9) {
          setIsRunning(false);
          setTaskid("");
          setCurrentStep(0);
        }
      }
    } catch (error) {
      console.error("Failed to fetch job progress", error);
      setIsRunning(false);
        setTaskid("");
        setCurrentStep(0);
    }
  };

   // Polling the progress every 2 seconds while the job is running
   useEffect(() => {
    if (isRunning) {
      const interval = setInterval(fetchProgress, 2000);
      return () => clearInterval(interval);
    }
  }, [isRunning]);

  console.log(`isRunning: ${isRunning}, currentStep: ${currentStep}`);

  return (
    <div>
      <button onClick={startBackgroundJob} disabled={isRunning} className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
        {isRunning ? "Job in Progress..." : "Start Background Job"}
      </button>

      <p>
        {currentStep < 10
          ? `Your background task has completed step ${currentStep} of 9.`
          : "Background task completed successfully!"}
      </p>

    </div>
  );
}
