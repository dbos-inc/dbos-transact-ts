import Image from "next/image";
import BackGroundTask from "@/components/client/BackGroundTask";
import CallDBOSWorkflow from "@/components/client/callDBOSWorkflow";

export default function Home() {
  return (
    <div className="grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20 font-[family-name:var(--font-geist-sans)]">
      <main className="flex flex-col gap-8 row-start-2 items-center sm:items-start">


          <h1 className="text-xl font-semibold mb-4">Welcome to DBOS!</h1>
          
    
          <p className="mb-4">
             DBOS helps you build applications that are <strong>resilient to any failure</strong>&mdash;no matter how many times you crash this app, your background task will always recover from its last completed step in about ten seconds.
         </p>
         <div>
            <CallDBOSWorkflow wfResult="" />
          </div>
          <div>
            <BackGroundTask />
          </div>
        </main>
      <footer className="row-start-3 flex gap-6 flex-wrap items-center justify-center">
        <a
          className="flex items-center gap-2 hover:underline hover:underline-offset-4"
          href="https://docs.dbos.dev/"
          target="_blank"
          rel="noopener noreferrer"
        >
          <Image
            aria-hidden
            src="/file.svg"
            alt="File icon"
            width={16}
            height={16}
          />
          Learn
        </a>
        <a
          className="flex items-center gap-2 hover:underline hover:underline-offset-4"
          href="https://docs.dbos.dev/#example-applications"
          target="_blank"
          rel="noopener noreferrer"
        >
          <Image
            aria-hidden
            src="/window.svg"
            alt="Window icon"
            width={16}
            height={16}
          />
          Examples
        </a>
        <a
          className="flex items-center gap-2 hover:underline hover:underline-offset-4"
          href="https://www.dbos.dev/"
          target="_blank"
          rel="noopener noreferrer"
        >
          <Image
            aria-hidden
            src="/globe.svg"
            alt="Globe icon"
            width={16}
            height={16}
          />
          Go to dbos.dev →
        </a>
      </footer>
    </div>
  );
}
