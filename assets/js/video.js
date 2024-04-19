var poster = document.getElementById('videoposter');
var number = Math.floor(Math.random() * 4) + 1;
poster.src += './img/video/'+ number +'.mp4';