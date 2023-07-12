// Declare Operon interface
export interface Operon {
  helloWorld: () => void;
}

// Create operon object with helloWorld function
export const operon: Operon = {
  helloWorld: function() {
    console.log('[Operon] Hello, world!');
  }
};