//global vars

var divs = document.querySelectorAll('div div');
var randomIndex = Math.floor(Math.random()*6);
var easyRandomIndex = Math.floor(Math.random()*3)
var colorToGuess = null;
var gameOver = false;
var easyModeBool = false;
var button = document.querySelector('#reset');
var easyModeButton = document.querySelector('#bEasy');
var hardModeButton = document.querySelector('#bHard');
//Assign all divs colors, dependence on Change color

//we start in hard mode
hardMode();
hardModeButton.classList.add('selected');

function processDivColors(){


for( var i = 0; i < divs.length; i++)
{
	//change color of each div
	console.log(i);
	changeColor(divs[i]);


}

}

//change div colors to random colors // dependence Math lib

function changeColor(div){
	div.style.background = "rgb(" + Math.floor(Math.random()*255) + ", " + Math.floor(Math.random()*255)+", " + Math.floor(Math.random()*255)+")";
}

//generate guess color each time 
function generateGuessColor(){
//select color as reference
if(easyModeBool){
	colorToGuess = divs[easyRandomIndex].getAttribute('style').substring(divs[easyRandomIndex].getAttribute('style').indexOf(':')+2,divs[easyRandomIndex].getAttribute('style').indexOf(';'));
}

else{

	colorToGuess = divs[randomIndex].getAttribute('style').substring(divs[randomIndex].getAttribute('style').indexOf(':')+2,divs[randomIndex].getAttribute('style').indexOf(';'));
}

//set span color to guess
placeGuessColor(colorToGuess);
}

//Placing guess color

function placeGuessColor(colorToGuess){

	var guessColorSpace = document.querySelector('h1 span');
	guessColorSpace.textContent = ' '+ colorToGuess;
}

//execute process div function
processDivColors();
//generate guess Color
generateGuessColor();




//fad color if color selected is not the color in the title.

//function to get color of clicked div

function processColor(){
	if(!gameOver){
	if (easyModeBool)
	{
	var color = this.getAttribute('style').substring(divs[easyRandomIndex].getAttribute('style').indexOf(':')+2,divs[easyRandomIndex].getAttribute('style').indexOf(';'));
	}

	else{

	var color = this.getAttribute('style').substring(divs[randomIndex].getAttribute('style').indexOf(':')+2,divs[randomIndex].getAttribute('style').indexOf(';'));
	}
	var colorBool = color == colorToGuess;
	if (colorBool){
		updateMessage(colorBool);
		changeAllColors();
		gameOver = true;
		button.textContent = 'Play Again?';
		return;
	}

	else{

		this.style.background = '#232323';
		updateMessage(colorBool);
		return;
	}
	}
	else{

		return;
	}

}

function updateMessage(colorBool){

	var messageSpan = document.getElementById('result');

	if(colorBool){
		messageSpan.textContent = 'Correct!';

	}

	else{

		messageSpan.textContent = 'Try again!';
	}
	
}

function changeAllColors(){

	for(var i = 0; i < divs.length; i++){

			//change squares
		divs[i].style.background = colorToGuess;

		//change h1

		var h1 = document.querySelector('h1');
		h1.style.background = colorToGuess;
	}


}

//function reset when complete or when the use wants new colors

function reset(){

processDivColors();
generateGuessColor();
//reset h1 color 
var h1 = document.querySelector('h1');
h1.style.background = 'steelblue';
//reset message
var messageSpan = document.getElementById('result');
messageSpan.textContent = '';

//modify button text content

button.textContent = 'New Colors';

//game over

gameOver = false;

}

//function for easy mode 

function easyMode(){
	gameOver = false;
	//remove hard mode selection and add easy mode

	hardModeButton.classList.remove('selected');
	easyModeButton.classList.add('selected');
	
	//darkendivs 3-5 and remove event listeners
	if(!easyModeBool)
	{
		for (var j = 3; j < divs.length; j++)
		{
			divs[j].style.background = '#232323';
			divs[j].removeEventListener('click', processColor);
		}

		
	}
	easyModeBool = true;

	//regenerate 3 squares

	divs = [divs[0], divs[1], divs[2]];
	processDivColors();
	generateGuessColor();

	for (var i = 0; i < divs.length; i++)
{
	console.log('easy Is :' + i);
	divs[i].addEventListener('click', processColor);
}
	button.addEventListener('click', reset);



}

function hardMode(){
	gameOver = false;
	easyModeBool = false;

	hardModeButton.classList.add('selected');
	easyModeButton.classList.remove('selected');


	
	divs = document.querySelectorAll('div div');
	processDivColors();
	generateGuessColor();
		for (var i = 0; i < divs.length; i++)
{
	console.log('hard Is :' + i);
	divs[i].addEventListener('click', processColor);
}
	button.addEventListener('click', reset);

}



button.addEventListener('click', reset);

//event listener for east mode

easyModeButton.addEventListener('click', easyMode);
hardModeButton.addEventListener('click', hardMode);



