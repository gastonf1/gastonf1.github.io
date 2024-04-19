var poster = document.getElementById('videoposter');
var number = Math.floor(Math.random() * 7) + 1;
poster.src += './img/video/'+ number +'.mp4';