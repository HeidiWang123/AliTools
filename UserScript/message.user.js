// ==UserScript==
// @id             message.user.js@everyx
// @name           阿里询盘界面优化
// @version        1.0
// @namespace      http://userscript.everyx.in/message.user.js
// @author         everyx
// @description    阿里询盘界面优化
// @include        http://message.alibaba.com/message/default.htm#feedback/detail/all/*
// @run-at         window-load
// @updateURL      https://raw.githubusercontent.com/everyx/AliTools/master/message.user.js
// ==/UserScript==

main();

const WIDTH_TUNING = 2;

function main() {
  setTimeout(addCenterDetailToogle(), 3000);
}

function addCenterDetailToogle() {

  var resizeHandle = document.querySelector('.aui-detail-resize-handler');
  var detailCenter = document.querySelector('.aui-detail-center');
  var detailRight = document.querySelector('.aui-detail-right');

  var rightWidth = detailRight.offsetWidth;
  var centerWidth = detailCenter.offsetWidth;

  resizeHandle.ondblclick = function() {
    if (detailCenter.style.display === "none") {
      display = "";
      width = rightWidth - centerWidth + WIDTH_TUNING;
    } else {
      display = "none";
      width = rightWidth + centerWidth - WIDTH_TUNING;
    }

    detailRight.style.width = width + "px";
    detailCenter.style.display = display;

    rightWidth = width;
  };
}
