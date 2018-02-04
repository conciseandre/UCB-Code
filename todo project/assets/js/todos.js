//Selected Features

$('ul').on('click', 'li', complete);

$('ul').on('click', "li span[class ='delete']", remove);

$("input[type ='text']").on('keypress', addToDo);

$("h1 i").on('click', toggle);

//Helper functions

function complete(){

	$(this).toggleClass('complete');
}

function remove(e){
	$(this).parent().fadeOut('1000', function(){
		$(this).remove();
	});
	e.stopPropagation();
}

function addToDo(e){
//take input from input upon enter key
	if(e.which == 13){
		var input = $(this).val();
		$("<li><span class='delete'><i class ='fa fa-trash'></i> </span>" + input + "</li>").appendTo('ul');
		$(this).val('');
	}
	
}

function toggle(){

	$('input').fadeToggle('1000');
}

