-- CreateTable
CREATE TABLE "dbos_hello" (
    "greeting_id" SERIAL NOT NULL,
    "greeting" TEXT NOT NULL,

    CONSTRAINT "dbos_hello_pkey" PRIMARY KEY ("greeting_id")
);
