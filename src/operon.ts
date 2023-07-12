// Declare Operon interface
export interface Operon {
    helloWorld: () => void;
}

// Create operon object with helloWorld function
const operon: Operon = {
    helloWorld: function() {
        console.log('Hello, world!');
    }
};

export default operon;