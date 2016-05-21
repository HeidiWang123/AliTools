// ==UserScript==
// @id             p4p.user.js@everyx
// @name           P4P 出价助手
// @version        0.1
// @namespace      http://userscript.everyx.in/p4p.user.js
// @author         everyx
// @description    A safe way to use baidu.
// @include        http://www2.alibaba.com/manage_ad_keyword.htm*
// @run-at         document-end
// @updateURL      https://raw.githubusercontent.com/everyx/AliTools/master/p4p.user.js
// ==/UserScript==
main();

function main() {
  p4pBid();
}

function p4pBid() {
  addBidButton();
}

function addBidButton() {
  var toolbar = document.querySelector('.toolbar');
  var bidButton = document.createElement("span");
  bidButton.innerHTML = '<a style="border-color: red;"  class="ui2-button ui2-button-normal ui2-button-medium">自动调价</a>';
  bidButton.className  = 'btn';
  bidButton.addEventListener("click", doBid);
  toolbar.appendChild(bidButton);
}

function doBid() {
  document.querySelector('span[data-value="in_promotion"]').click();
  doiteratorBidAfterLoaded();
}

function doiteratorBidAfterLoaded() {
  var loadingPanel = document.querySelector('.bp-loading-panel');
  var judgeFunc = function() {
    if (loadingPanel.style.display == 'none') {
      return true;
    } else {
      return false;
    }
  }
  wait(1000, judgeFunc, iteratorBid);
}

function iteratorBid() {
  GM_log("iteratorBid");
  var changePriceHrefs = document.querySelectorAll('a[data-role="btn-change-price"]');
  iteratorChangePrice(0, changePriceHrefs);
}

function iteratorChangePrice(i, nodeList) {
  item = nodeList[i];
  item.click();

  var target = document.querySelector('div[data-role="rank-infos"]');
  var starLevel = item.parentNode.parentNode.querySelector('i.bp-icon-qsstar').className.replace(/[A-Za-z- ]*/, "");
  var changeCurrentPriceWrapper = function() {
    changeCurrentPrice(starLevel);
    target.removeEventListener("DOMSubtreeModified", changeCurrentPriceWrapper);
  };
  target.addEventListener("DOMSubtreeModified", changeCurrentPriceWrapper);

  if (i === (nodeList.length-1)) {
    var nextButton = document.querySelector('a.next[data-role="next"]');
    if (nextButton) {
      setTimeout(function() {
        nextButton.click();
        doiteratorBidAfterLoaded();
      }, 3000);
    }
  }

  setTimeout(function() {
    i = i+1;
    if (i < nodeList.length) {
      iteratorChangePrice(i, nodeList);
    }
  }, 3000);
}

var MINIMUM_PRICE = 3.0;
var FIVE_STAR_TOP_PRICE = 6.6;
var FOUR_STAR_TOP_PRICE = 5.7;

function getLowestPrice() {

  var top5Prices = document.querySelectorAll('div.ui2-dialog tbody td a');
  for (i = top5Prices.length-1; i >= 0; i--) {
    if (top5Prices[i].dataset.value != '--') {
      return top5Prices[i].dataset.value;
    }
  }
  return MINIMUM_PRICE;
}

function changeCurrentPrice(starLevel) {
  var confirmButton = document.querySelector('input.ui2-button[data-role="confirm"]');
  var cancelButton = document.querySelector('input.ui2-button[data-role="cancel"]');

  if (starLevel != '5' && starLevel != '4') {
    cancelButton.click();
    return;
  }


  var lowestPrice = getLowestPrice();
  var currentTopPrice = starLevel==5 ? FIVE_STAR_TOP_PRICE : FOUR_STAR_TOP_PRICE;


  var priceInput = document.querySelector('input[name="addPrice"]');
  if (lowestPrice > currentTopPrice) {
    priceInput.value = currentTopPrice;
  } else {
    priceInput.value = lowestPrice;
  }
  confirmButton.click();
}

function wait(delay, judgeFunc, func) {
  var interval = setInterval(function() {
    if (judgeFunc()) {
      clearInterval(interval);
      func();
    }
  }, delay);
}
