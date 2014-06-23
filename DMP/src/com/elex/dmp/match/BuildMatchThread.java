package com.elex.dmp.match;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class BuildMatchThread implements Runnable {
	
	private List<DspClassDescriptor> dspClassList;
	private List<DmpTopicDescriptor> dmpTopicList;
	CountDownLatch latch;

	public BuildMatchThread(List<DspClassDescriptor> dspClassList,
			List<DmpTopicDescriptor> dmpTopicList,CountDownLatch mainLatch) {
		super();
		this.dspClassList = dspClassList;
		this.dmpTopicList = dmpTopicList;
		this.latch=mainLatch;
	}

	
	
	@Override
	public void run() {
		DspDmpClassMatch match = new DspDmpClassMatch();
		match.buildMatch(dspClassList, dmpTopicList);
		latch.countDown();
	}
	
	

}
