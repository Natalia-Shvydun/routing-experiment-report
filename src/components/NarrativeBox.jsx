import React from 'react'
import './NarrativeBox.css'

function NarrativeBox() {
  return (
    <div className="narrative-box">
      <h4 className="narrative-headline">← The average is influenced by visitor mix changes.</h4>
      <div className="narrative-content">
        <p>
          The average is shaped by both how well our product works and who is visiting. 
          A drop in average doesn't always mean our product changes aren't working—it can 
          just reflect a change in the type of visitors we're getting.
        </p>
        <p>
          To understand if the type of visitors this year differs from last year, check the 
          Mix change in the Channel table below. Pay attention to the channels with big increases, 
          and is their ApV lower than the average? If yes, they are pulling the average down.
        </p>
      </div>
    </div>
  )
}

export default NarrativeBox
